# Introdution

horm is an ORM-like lib to map go struct to HBase columns,  
It provides CURD interface for HBase.

## Quick Start

```go
package main

import (
	"context"
	"fmt"

	"github.com/challenai/horm"
	"github.com/challenai/horm/client"
)

type User struct {
	*horm.Model
	Name string `hbase:"family,name"` // column family = family, column name = name
	Age  int    `hbase:"family,age"`  // column family = family, column name = age
}

func (*User) Namespace() string {
	return "namespace"
}

func (*User) TableName() string {
	return "user"
}

func main() {
	const (
		addr   = ""
		rowkey = "id0001"
	)

	ctx := context.Background()
	headers := []client.Header{
		{Key: "header1", Value: ""},
	}
	hb, err := horm.NewHBase(addr, headers)
	if err != nil {
		panic(err)
	}

	user := &User{}
	err = hb.Get(ctx, user, rowkey).Error
	if err != nil {
		println(err)
		return
	}
	fmt.Println(user.Name, user.Age)
}
```
