"""Convert copy-initialization `T x = expr` to list-initialization `T x{expr}`.

Conservative source-to-source rewrite driven by a small tokenizer:
- skips strings, char literals, raw strings, comments, preprocessor lines
- skips `=` inside parens unless the parens belong to for/if/while/switch
- converts init-captures `[x = e]` -> `[x{e}]`
- skips enum bodies, structured bindings, designated initializers,
  `using`/`namespace`/`template`/`concept`/`return`-style contexts,
  `= default`/`= delete`/pure-virtual, and plain assignments
- skips `auto x = {...}` (braced RHS would change deduction)
- supports multi-declarator declarations `int a = 1, b = 2`
"""

import sys
import re

KEYWORDS_BLOCK = {
    'return', 'throw', 'case', 'goto', 'else', 'do', 'while', 'for', 'if',
    'switch', 'catch', 'try', 'new', 'delete', 'sizeof', 'alignof',
    'decltype', 'noexcept', 'static_assert', 'typeid', 'asm', 'namespace',
    'using', 'typedef', 'template', 'operator', 'concept', 'requires',
    'co_await', 'co_yield', 'co_return', 'static_cast', 'dynamic_cast',
    'const_cast', 'reinterpret_cast', 'export', 'import', 'module',
    'and', 'or', 'not', 'xor', 'and_eq', 'or_eq', 'xor_eq', 'not_eq',
    'bitand', 'bitor', 'compl',
}

QUALIFIERS = {
    'const', 'volatile', 'static', 'mutable', 'inline', 'extern',
    'constexpr', 'consteval', 'constinit', 'register', 'thread_local',
    'friend', 'virtual', 'explicit', 'typename', 'export',
}


def split_top_commas(s):
    parts = []
    depth = 0
    last = 0
    for i, c in enumerate(s):
        if c in '<{[(':
            depth += 1
        elif c in '>}] )'.replace(' ', ''):
            depth -= 1
        elif c == ',' and depth == 0:
            parts.append(s[last:i])
            last = i + 1
    parts.append(s[last:])
    return parts


def angle_balanced(s):
    depth = 0
    for c in s:
        if c == '<':
            depth += 1
        elif c == '>':
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def strip_square(s):
    return re.sub(r'\[[^\[\]]*\]', ' ', s)


def strip_angle(s):
    prev = None
    while prev != s:
        prev = s
        s = re.sub(r'<[^<>]*>', ' ', s)
    return s


def strip_curly(s):
    prev = None
    while prev != s:
        prev = s
        s = re.sub(r'\{[^{}]*\}', ' ', s)
    return s


def head_is_decl(head):
    """specifiers ... [*&] name   -> looks like a declaration."""
    h = strip_square(head)
    if not angle_balanced(h):
        return False
    h = strip_angle(h)
    for bad in ('(', ')', '.', '-', '+', '=', '{', '}', '?', '"', "'", '#',
                '/', '%', '^', '|', '~', '!', ','):
        if bad in h:
            return False
    m = re.search(r'([A-Za-z_]\w*)\s*$', h)
    if not m:
        return False
    name = m.group(1)
    if name in KEYWORDS_BLOCK or name == 'auto':
        return False
    spec = h[:m.start()].rstrip(' *&')
    if not re.search(r'[A-Za-z_]', spec):
        return False
    if spec.rstrip().endswith(':'):
        return False
    return not any(w in KEYWORDS_BLOCK for w in re.findall(r'[A-Za-z_]\w*', spec))


def seg_is_declarator(seg):
    """plain declarator (possibly already initialized): `name`, `*p`, `a[3]`,
    `b = 2`, `c{3}` -- but not a full decl with specifiers."""
    head = re.split(r'[={]', seg, maxsplit=1)[0]
    return re.fullmatch(r'\s*[&*\s]*[A-Za-z_]\w*(\s*\[[^\]]*\])?\s*',
                        head) is not None


def strip_comments(s):
    s = re.sub(r'/\*.*?\*/', ' ', s, flags=re.DOTALL)
    return re.sub(r'//[^\n]*', ' ', s)


def looks_like_declaration(chunk):
    s = strip_curly(strip_comments(chunk))
    parts = split_top_commas(s)
    if len(parts) == 1:
        return head_is_decl(parts[0])
    if not head_is_decl(re.split(r'[={]', parts[0], maxsplit=1)[0]):
        return False
    return all(seg_is_declarator(p) for p in parts[1:])


def is_auto_specifier(chunk):
    s = strip_curly(strip_comments(chunk))
    s = strip_square(s)
    last = split_top_commas(s)[-1]
    head = re.split(r'[={]', last, maxsplit=1)[0]
    h = strip_angle(head)
    m = re.search(r'([A-Za-z_]\w*)\s*$', h)
    if not m:
        return False
    spec = h[:m.start()]
    words = [w for w in re.findall(r'[A-Za-z_]\w*', spec)
             if w not in QUALIFIERS]
    return words == ['auto']


def is_full_braced(init):
    """init.strip() is exactly one balanced {...} group."""
    s = init.strip()
    if not s.startswith('{'):
        return False
    depth = 0
    i = 0
    n = len(s)
    st = 'code'
    while i < n:
        c = s[i]
        if st == 'code':
            if c == '/' and s[i + 1:i + 2] == '/':
                st = 'line';
                i += 1
            elif c == '/' and s[i + 1:i + 2] == '*':
                st = 'block';
                i += 1
            elif c == '"':
                st = 'str'
            elif c == "'":
                st = 'chr'
            elif c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    return i == n - 1
        elif st == 'line':
            if c == '\n':
                st = 'code'
        elif st == 'block':
            if c == '*' and s[i + 1:i + 2] == '/':
                st = 'code';
                i += 1
        elif st == 'str':
            if c == '\\':
                i += 1
            elif c == '"':
                st = 'code'
        elif st == 'chr':
            if c == '\\':
                i += 1
            elif c == "'":
                st = 'code'
        i += 1
    return False


def scan_terminator(src, pos):
    """Find ; , ) ] } at depth 0 starting at pos. Returns (term, sig_end)."""
    n = len(src)
    j = pos
    depth = 0
    st = 'code'
    rend = ''
    sig_end = pos
    while j < n:
        c = src[j]
        if st == 'code':
            if c == '/' and src[j + 1:j + 2] == '/':
                st = 'line';
                j += 1
            elif c == '/' and src[j + 1:j + 2] == '*':
                st = 'block';
                j += 1
            elif c == '"':
                st = 'str'
            elif c == "'":
                if j > 0 and j + 1 < n and src[j - 1].isdigit() \
                        and src[j + 1].isdigit():
                    sig_end = j + 1
                else:
                    st = 'chr'
            elif c == 'R' and src[j + 1:j + 2] == '"' and (
                    j == 0 or not (src[j - 1].isalnum() or src[j - 1] == '_')):
                k = j + 2
                while k < n and src[k] != '(':
                    k += 1
                rend = src[j + 2:k]
                st = 'raw'
                sig_end = j + 1
                j = k
            elif c in '([{':
                depth += 1
                sig_end = j + 1
            elif c in ')]}':
                if depth == 0:
                    return j, sig_end
                depth -= 1
                sig_end = j + 1
            elif c in ';,' and depth == 0:
                return j, sig_end
            elif not c.isspace():
                sig_end = j + 1
        elif st == 'line':
            if c == '\n':
                st = 'code'
        elif st == 'block':
            if c == '*' and src[j + 1:j + 2] == '/':
                st = 'code';
                j += 1
        elif st == 'str':
            if c == '\\':
                j += 1
            elif c == '"':
                st = 'code';
                sig_end = j + 1
        elif st == 'chr':
            if c == '\\':
                j += 1
            elif c == "'":
                st = 'code';
                sig_end = j + 1
        elif st == 'raw':
            if c == ')' and src.startswith(rend + '"', j + 1):
                st = 'code'
                sig_end = j + len(rend) + 2
                j += len(rend) + 1
        j += 1
    return None, None


def transform(src):
    n = len(src)
    i = 0
    line_start = True
    st = 'code'
    rend = ''
    stack = []  # ('(',ctrl) ('[',cap) ('{',enum)
    boundary = {0: 0}
    edits = []
    skipped = []

    def prev_ns(pos):
        j = pos - 1
        while j >= 0 and src[j] in ' \t\r\n':
            j -= 1
        return j

    def paren_owner(pos):
        j = prev_ns(pos)
        m = re.search(r'([A-Za-z_]\w*)\s*$', src[:j + 1])
        if not m:
            return False
        w = m.group(1)
        if w == 'constexpr':
            m2 = re.search(r'([A-Za-z_]\w*)\s*$', src[:m.start()])
            w = m2.group(1) if m2 else w
        return w in ('for', 'if', 'while', 'switch')

    def capture_bracket(pos):
        j = prev_ns(pos)
        if j < 0:
            return True
        c = src[j]
        return not (c.isalnum() or c == '_' or c in ')][')

    def enum_brace(pos):
        j = prev_ns(pos)
        tail = src[max(0, j - 200):j + 1]
        return re.search(
            r'\benum(\s+(class|struct))?(\s+\w+)?(\s*:\s*[\w:,\s]+)?\s*$',
            tail) is not None

    def context():
        if not stack:
            return 'code'
        top = stack[-1]
        if top[0] == '{':
            return 'enum' if top[1] else 'code'
        if top[0] == '(':
            return 'ctrl' if top[1] else 'paren'
        return 'capture' if top[1] else 'sub'

    def try_edit(i):
        ctx = context()
        if ctx in ('paren', 'sub', 'enum'):
            return
        start = boundary.get(len(stack), 0)
        chunk = src[start:i]
        ok = validate_capture(chunk) if ctx == 'capture' \
            else looks_like_declaration(chunk)
        if not ok:
            skipped.append((i, ctx, src[max(0, i - 40):i + 30]))
            return
        term, sig = scan_terminator(src, i + 1)
        if term is None:
            skipped.append((i, 'noterm', src[max(0, i - 40):i + 30]))
            return
        init = src[i + 1:sig]
        if ctx != 'capture':
            if is_auto_specifier(chunk) and init.strip().startswith('{'):
                skipped.append((i, 'auto-brace', ''))
                return
            kind = 'strip' if is_full_braced(init) else 'wrap'
        else:
            kind = 'wrap'
        edits.append((i, term, sig, kind))

    while i < n:
        c = src[i]
        if st == 'code':
            if c == '\n':
                line_start = True
            elif c == '#' and line_start:
                j = i
                while j < n:
                    if src[j] == '\n':
                        k = j - 1
                        while k > i and src[k] in ' \t\r':
                            k -= 1
                        if src[k] != '\\':
                            break
                    j += 1
                i = j
                continue
            elif not c.isspace():
                line_start = False
            if c == '/' and src[i + 1:i + 2] == '/':
                st = 'line';
                i += 1
            elif c == '/' and src[i + 1:i + 2] == '*':
                st = 'block';
                i += 1
            elif c == '"':
                st = 'str'
            elif c == "'":
                if i > 0 and i + 1 < n and src[i - 1].isdigit() \
                        and src[i + 1].isdigit():
                    pass
                else:
                    st = 'chr'
            elif c == 'R' and src[i + 1:i + 2] == '"' and (
                    i == 0 or not (src[i - 1].isalnum() or src[i - 1] == '_')):
                k = i + 2
                while k < n and src[k] != '(':
                    k += 1
                rend = src[i + 2:k]
                st = 'raw'
                i = k
            elif c == '(':
                stack.append(('(', paren_owner(i)))
                boundary[len(stack)] = i + 1
            elif c == '[':
                stack.append(('[', capture_bracket(i)))
                boundary[len(stack)] = i + 1
            elif c == '{':
                stack.append(('{', enum_brace(i)))
                boundary[len(stack)] = i + 1
            elif c in ')]}':
                if stack:
                    stack.pop()
                    boundary.pop(len(stack) + 1, None)
                if c == '}':
                    boundary[len(stack)] = i + 1
            elif c == ';':
                boundary[len(stack)] = i + 1
            elif c == ':':
                if src[i - 1:i] != ':' and src[i + 1:i + 2] != ':':
                    boundary[len(stack)] = i + 1
            elif c == '?':
                boundary[len(stack)] = i + 1
            elif c == '=':
                pc = src[prev_ns(i)] if prev_ns(i) >= 0 else ''
                if pc not in '=<>!+-*/%&|^~' and src[i + 1:i + 2] != '=':
                    m = re.search(r'([A-Za-z_]\w*)\s*$',
                                  src[:prev_ns(i) + 1])
                    if not (m and m.group(1) == 'operator'):
                        try_edit(i)
        elif st == 'line':
            if c == '\n':
                st = 'code';
                line_start = True
        elif st == 'block':
            if c == '*' and src[i + 1:i + 2] == '/':
                st = 'code';
                i += 1
        elif st == 'str':
            if c == '\\':
                i += 1
            elif c == '"':
                st = 'code'
        elif st == 'chr':
            if c == '\\':
                i += 1
            elif c == "'":
                st = 'code'
        elif st == 'raw':
            if c == ')' and src.startswith(rend + '"', i + 1):
                st = 'code';
                i += len(rend) + 1
        i += 1

    return edits, skipped


def validate_capture(chunk):
    s = strip_curly(strip_comments(chunk))
    parts = split_top_commas(s)
    for p in parts[:-1]:
        if not seg_is_declarator(p) and p.strip() not in ('this', '*this', ''):
            # earlier segs may be plain names or this
            if not re.fullmatch(r'\s*[&*]?\s*(this|[A-Za-z_]\w*)\s*', p):
                return False
    last = parts[-1]
    return re.fullmatch(r'\s*[&*]?\s*[A-Za-z_]\w*\s*', last) is not None


def apply_region(src, edits, start, end):
    """edits sorted by position; nested edits apply inside their parent's init."""
    out = []
    last = start
    i = 0
    m = len(edits)
    while i < m:
        eq, term, sig, kind = edits[i]
        j = i + 1
        inner = []
        while j < m and edits[j][0] < sig:
            inner.append(edits[j])
            j += 1
        out.append(src[last:eq].rstrip())
        init = apply_region(src, inner, eq + 1, sig).strip()
        out.append('{' + init + '}' if kind == 'wrap' else init)
        last = sig
        i = j
    out.append(src[last:end])
    return ''.join(out)


def apply_edits(src, edits):
    return apply_region(src, edits, 0, len(src))


def main():
    import os
    dry = '--dry-run' in sys.argv
    files = []
    for a in sys.argv[1:]:
        if a == '--dry-run':
            continue
        if os.path.isdir(a):
            for root, _, names in os.walk(a):
                files += [os.path.join(root, n) for n in names
                          if n.endswith(('.hpp', '.cpp'))]
        else:
            files.append(a)
    total = 0
    for path in files:
        with open(path, 'r', encoding='utf-8') as f:
            src = f.read()
        edits, skipped = transform(src)
        total += len(edits)
        if dry:
            for eq, term, sig, kind in edits:
                line = src.count('\n', 0, eq) + 1
                head = src[max(0, eq - 50):eq].split('\n')[-1]
                init = src[eq + 1:sig].strip().split('\n')[0][:60]
                print(f'{path}:{line}: [{kind}] {head.strip()} | {init}')
            for eq, why, ctx in skipped:
                if why in ('paren', 'sub', 'enum', 'noterm'):
                    continue
                line = src.count('\n', 0, eq) + 1
                print(f'{path}:{line}: SKIP-{why} | {ctx.strip()[-70:]}')
        else:
            with open(path, 'w', encoding='utf-8', newline='') as f:
                f.write(apply_edits(src, edits))
        print(f'{path}: {len(edits)} converted, {len(skipped)} skipped')
    print('total', total)


if __name__ == '__main__':
    main()
