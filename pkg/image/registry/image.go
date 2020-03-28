package main

import (
    "bytes"
    "fmt"
    "os"
    "regexp"
    "strings"
    docker "github.com/fsouza/go-dockerclient"
)
type Image struct {
    Registry string
    Project string
    Name string
    Tag string
    Message *Message
}
type Message struct {
    Mes string
    Type string
    Code int
}
func main() {
    //var endpoint string = "http://192.168.66.123:2375/v1.24"
    var endpoint string = "unix:///var/run/docker.sock"
    var client *docker.Client
    var err error

    client, err = docker.NewClient(endpoint)
    if err != nil {
        panic(err)
    }

    //listImages(client)
    //listContainers(client)
    //getContainerLogs(client)

    var this *Image
    mes, err := this.LoadImage(client)
    if err != nil {
        fmt.Println("Load Image is fail")
    }else {
        fmt.Println(mes.Mes)
    }
    mes, err = this.TagImage(client)
    if err != nil {
        fmt.Println("Tag Image is fail")
    }else {
        fmt.Println(mes.Mes)
    }
    mes, err = this.PushImageCustomRegistry(client)
    if err != nil {
        fmt.Println("Push Image is fail")
    }else {
        fmt.Println(mes.Mes)
       // fmt.Println((*this).Name)
    }


}

// load image
func (this *Image) LoadImage(client * docker.Client) (*Message, error){
     mes := new(Message)
     mes.Type = "load"
     this = & Image{
         Message : mes,
     }
    tar, err := os.Open("/root/go/docker/busybox.tar")
    if err != nil {
        mes.Mes = "open file error"
        return mes, err
    }
    defer tar.Close()
    err = client.LoadImage(docker.LoadImageOptions{InputStream: tar})
    if err != nil {
        mes.Mes = "load image error"
        return mes, err
    } else {
        mes.Mes = "load image success"
        mes.Code = 200 //执行成功
        return mes, nil
    }
}

//Tag iamge
func (this *Image)TagImage(client * docker.Client) (*Message, error){
    mes := new(Message)
    mes.Type = "tag"
    this = &Image{
        Tag:  "latest",
        Name: "registry.cn-beijing.aliyuncs.com/hiacloud/busybox",
        Project: "hiacloud",
        Registry: "registry.cn-beijing.aliyuncs.com",
        Message:  mes,
    }
    err := client.TagImage("busybox:latest", docker.TagImageOptions{
        Repo: "registry.cn-beijing.aliyuncs.com/hiacloud/busybox",
        Tag: "1.29.1",
    })
    if err != nil && !strings.Contains(err.Error(), "tag image fail") {
        mes.Mes = "tag image error"
        return mes, err
    }else {
        mes.Mes = "tag image success"
        mes.Code = 200 //执行成功
        return mes, nil
    }
}
// push image
func (this *Image)PushImageCustomRegistry(client * docker.Client) (*Message, error){
    mes := new(Message)
    mes.Type = "push"
    this = &Image{
        Tag:  "latest",
        Name: "registry.cn-beijing.aliyuncs.com/hiacloud/busybox",
        Project: "hiacloud",
        Registry: "registry.cn-beijing.aliyuncs.com",
        Message:  mes,
    }
    var buf bytes.Buffer
    registryAuth := docker.AuthConfiguration{
        Username: "bjyimaike@163.com",
        Password: "emcc7556",
        ServerAddress: "registry.cn-beijing.aliyuncs.com",
    }
    opts := docker.PushImageOptions{
        Name: "registry.cn-beijing.aliyuncs.com/hiacloud/busybox",
        Tag: "1.29.1",
        Registry: "registry.cn-beijing.aliyuncs.com",
        RawJSONStream: true,
        OutputStream: &buf,
    }
   if err := client.PushImage(opts, registryAuth); err != nil {
       mes.Mes = "push image error"
       return mes, err
   }else {
       mes.Mes = "push image success"
       return mes, nil
   }
}

func listImages(client * docker.Client) {
    opts := docker.ListImagesOptions{All: false}

    images, err := client.ListImages(opts)
    if err != nil {
        panic(err)
    }
    for _, image := range images {
        fmt.Println("Image  ID      : ", image.ID)
        fmt.Println("Imager RepoTags: ", image.RepoTags)
    }
}

func listContainers(client * docker.Client) {
    opts := docker.ListContainersOptions{}

    containers, err := client.ListContainers(opts)
    if err != nil {
        panic(err)
    }
    for _, container := range containers {
        fmt.Println("Container ID   : ", container.ID)
        fmt.Println("Container Names: ", container.Names)
    }
}


func getContainerLogs(client * docker.Client) {
    var buf bytes.Buffer
    opts := docker.LogsOptions {
        Container:      "<containername>",
        OutputStream: &buf,
        ErrorStream:  &buf,
        Follow:       false,
        Stdout:       true,
        Stderr:       true,
//      Timestamps:   true,
        Tail:         "10",
    }

    err := client.Logs(opts)
    if err != nil {
        panic(err)
    }

    lines := strings.Split(buf.String(), "\n")
    for _, line := range lines {
        if matched, _ := regexp.MatchString(`: ERROR  : `, line); matched {
            fmt.Println(line)
        }
    }
}



