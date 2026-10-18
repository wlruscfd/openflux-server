//go:build windows

package outbound

import "syscall"

func processAttributes() *syscall.SysProcAttr { return nil }

func terminateSignal() syscall.Signal { return syscall.Signal(0x15) }
