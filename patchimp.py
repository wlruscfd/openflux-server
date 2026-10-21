import io
p = r"C:\Users\savin\AppData\Local\Temp\opencode\ofx-scratch\outbound\dumpconfig_test.go"
s = io.open(p, encoding="utf-8").read()
s = s.replace('import (\n\t"os"', 'import (\n\t"os"\n\t"strconv"')
io.open(p, "w", encoding="utf-8", newline="").write(s)
