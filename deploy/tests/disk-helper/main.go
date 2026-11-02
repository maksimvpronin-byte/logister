// A static test-only binary for the distroless shipper container. It can only
// fill or remove this test fixture, never arbitrary user-supplied paths.
package main

import("errors";"fmt";"os";"syscall")

const fixture="/var/lib/fluent-bit/test-fill"
func main(){
 if len(os.Args)!=2{os.Exit(2)}
 if os.Args[1]=="free"{if err:=os.Remove(fixture);err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(1)};return}
 if os.Args[1]!="fill"{os.Exit(2)}
 file,err:=os.Create(fixture);if err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(1)};defer file.Close()
 block:=make([]byte,1<<20)
 for i:=0;i<32;i++{if _,err:=file.Write(block);err!=nil{if errors.Is(err,syscall.ENOSPC){fmt.Println("ENOSPC");return};fmt.Fprintln(os.Stderr,err);os.Exit(1)}}
 fmt.Fprintln(os.Stderr,"quota did not fill within the 32 MiB test bound");os.Exit(1)
}
