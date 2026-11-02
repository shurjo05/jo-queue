// Command server will become the jo-queue HTTP server: the only process that
// talks to Redis. Workers and the CLI reach the queue through it (decision D2).
//
// M0: placeholder so the layout, build, and CI exist. Real code arrives in M3.
package main

import "fmt"

func main() {
	fmt.Println("jo-queue server: not implemented yet (planned for M3)")
}
