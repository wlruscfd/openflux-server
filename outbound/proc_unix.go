//go:build !windows

package outbound

import "syscall"

func processAttributes() *syscall.SysProcAttr { return &syscall.SysProcAttr{Setpgid: true} }

func terminateSignal() syscall.Signal { return syscall.SIGTERM }
