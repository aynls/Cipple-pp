from __future__ import annotations

from contextlib import ExitStack, closing
from pathlib import Path
import struct
import traceback
import uuid

from .protocol import (
    Conn, DISPATCH_REQUEST, HEARTBEAT_REQUEST, HEARTBEAT_RESPONSE, SUBSCRIBE_RESPONSE, decode_body,
    decode_bool_response, decode_dispatch, dispatch_response, frame, subscribe_request,
)
from .runtime import Child, cmdline, reserve_ports


RELAY_ADVERTISE = 21
RELAY_ROUTE = 22
RELAY_STATUS = 23
RELAY_HELLO = 24
TOPIC = b"relay", b"config"


def relay_status(token: uuid.UUID, ready: bool) -> bytes:
    return frame(RELAY_STATUS, token, bytes([ready]))


def subscribe(stack: ExitStack, port: int, listener: int = 0) -> Conn:
    control = stack.enter_context(closing(Conn("127.0.0.1", port, timeout=8)))
    control.send(frame(RELAY_ADVERTISE, uuid.uuid4(), struct.pack(">II", listener, 8 if listener else 0)))
    response = control.request(subscribe_request(uuid.uuid4(), *TOPIC, resume_after=0))
    assert decode_bool_response(response, SUBSCRIBE_RESPONSE)
    return control


def connect_to_captain(stack: ExitStack, control: Conn) -> Conn:
    kind, token, route = decode_body(control.read_frame())
    assert kind == RELAY_ROUTE and (route.blob(), route.blob()) == TOPIC
    peer = stack.enter_context(closing(Conn(route.text(), route.i32())))
    reply = peer.request(frame(RELAY_HELLO, token))
    assert decode_body(reply)[1] == token and decode_bool_response(reply, RELAY_STATUS)
    control.send(relay_status(token, True))
    heartbeat = control.request(frame(HEARTBEAT_REQUEST, uuid.uuid4()))
    assert decode_bool_response(heartbeat, HEARTBEAT_RESPONSE)
    return peer


def read_direct(control: Conn, authorizations: list[bytes], timeout: float | None = None):
    while True:
        body = control.read_frame(timeout)
        if body[0] == DISPATCH_REQUEST:
            return decode_dispatch(body)
        if body[0] == RELAY_STATUS:
            assert not decode_bool_response(body, RELAY_STATUS)
            continue
        assert body[0] == RELAY_ROUTE
        authorizations.append(body)


def run_relay(server: Path, client: Path, work: Path, env: dict[str, str]) -> list[dict]:
    work.mkdir(parents=True, exist_ok=True)
    port, spare_port = reserve_ports(2)
    endpoint = f"127.0.0.1:{port}"
    commands, observations = [], []

    def launch(binary, *args):
        command = [str(binary), *args]
        commands.append(cmdline(command))
        return Child(command, work, env)

    def publish(value):
        publisher = launch(client, endpoint, "put", "relay", "config", value)
        assert publisher.finish(8) == 0, publisher.text()

    status, detail = "pass", "组员走单跳转发，过半超时后更换队长，旧回执不能推进游标。"
    try:
        with ExitStack() as stack:
            service = launch(server, str(port), "server.db")
            stack.callback(service.kill)
            assert service.wait_for("Listening on", 8), service.text()
            captain = launch(client, "--state", "captain.db", "--relay-port", "0",
                             endpoint, "subscribe", "relay", "config")
            stack.callback(captain.kill)
            assert captain.wait_for("Subscribed", 8, "stdout"), captain.text()

            controls = [subscribe(stack, port), subscribe(stack, port), subscribe(stack, port, spare_port)]
            peers = [connect_to_captain(stack, control) for control in controls]
            value = "x" * 2048
            publish(value)
            deliveries = [decode_dispatch(peer.read_frame()) for peer in peers]
            original = deliveries[0]
            assert all(item == original for item in deliveries)
            assert original.entity.value == value.encode() and original.sequence > 0
            assert captain.wait_for(value, 5, "stdout"), captain.text()
            controls[0].send(dispatch_response(original.uuid, True))

            authorizations = [[], []]
            retries = [read_direct(control, auth) for control, auth in zip(controls[1:], authorizations)]
            assert authorizations[1], "expected the spare captain to authorize replacement links"
            for auth in authorizations[1]:
                kind, _, route = decode_body(auth)
                assert kind == RELAY_ROUTE and (route.blob(), route.blob()) == TOPIC
                assert route.blob() == b"" and route.i32() == 0
            for control, retry in zip(controls[1:], retries):
                assert retry.uuid != original.uuid and retry.entity == original.entity
                assert retry.sequence == original.sequence
                control.send(dispatch_response(original.uuid, True))

            publish("after-recovery")
            direct = read_direct(controls[0], [])
            assert direct.entity.value == b"after-recovery"
            controls[0].send(dispatch_response(direct.uuid, True))
            for control, auth in zip(controls[1:], authorizations):
                try:
                    read_direct(control, auth, 0.25)
                except TimeoutError:
                    continue
                raise AssertionError("the expired attempt acknowledged a newer retry")
            for control, retry in zip(controls[1:], retries):
                control.send(dispatch_response(retry.uuid, True))
                direct = read_direct(control, [])
                assert direct.entity.value == b"after-recovery" and direct.sequence > retry.sequence
                control.send(dispatch_response(direct.uuid, True))
            observations.append("3 个组员从队长收到同一投递，2 个超时组员经新的投递 ID 恢复。")
    except Exception:
        status, detail = "fail", traceback.format_exc().strip()
    return [{"name": "subscription_relay", "status": status, "detail": detail,
             "commands": commands, "observations": observations}]
