package util

import (
	"testing"
	"fmt"
)

func TestTry(t *testing.T) {
	a := 1
	{
		Try(func() {
			if a == 1 {
				panic(1)
				return
			}

		}).Finally(func() {
			fmt.Println("end finally")
		})
	}
}
