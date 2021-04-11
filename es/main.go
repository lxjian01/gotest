package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
)

// Elasticsearch demo

type Person struct {
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Married bool   `json:"married"`
}

func main() {
	client := createCLient()
	//createIndex(client,"persion")
	row := getRowById(client,"persion","1")
	persion := getPersion(row)
	fmt.Println(persion.Name,persion.Age,persion.Married)
	getRowsByIndex(client,"persion")

}

func createCLient() (client *elastic.Client){
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.204.129:9200"))
	if err != nil {
		// Handle error
		panic(err)
	}

	fmt.Println("connect to es success")
	return client
}

func getPersion(row json.RawMessage) *Person {
	persion := &Person{}
	err := json.Unmarshal(row, persion)
	if err != nil {
		fmt.Printf("byte convert string failed, err: %v", err)
	}
	return persion
}

//查找
func getRowById(client *elastic.Client,index string,id string) (row json.RawMessage) {
	//通过id查找
	get1, err := client.Get().Index(index).Id("1").Do(context.Background())
	if err != nil {
		panic(err)
	}
	if get1.Found {
		return get1.Source
	}else{
		return nil
	}
}

//查找
func getRowsByIndex(client *elastic.Client,index string){
	//通过id查找
	get1, err := client.GetMapping().Index(index).Do(context.Background())
	if err != nil {
		panic(err)
	}
	for country := range get1 {
		persion := &Person{}
		err := json.Unmarshal(get1["persion"], persion)
		if err != nil {
			fmt.Printf("byte convert string failed, err: %v", err)
		}
		fmt.Println(country, "首都是", country)


	}
	fmt.Println(get1)

}

func createIndex(client *elastic.Client,index string){
	p1 := Person{Name: "张三", Age: 20, Married: true}
	put1, err := client.Index().
		Index(index).
		Id("2").
		BodyJson(p1).
		Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
}