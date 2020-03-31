package upload

import (
    "bytes"
    "fmt"
    "github.com/liyongxian/go-hollicube/pkg/registry/docker"
    "os"
    _"regexp"
    "strings"
)
func NewClient() *docker.Client{
    //var endpoint string = "http://192.168.66.123:2375/v1.24"
    //var endpoint string = "unix:///var/run/docker.sock"
    if client, err := docker.NewClient("unix:///var/run/docker.sock"); err != nil {
        fmt.Println("New client is error")
        return nil
    }else {
        return client
    }
}

// load registry
func (this *Image) LoadImage(client * docker.Client, fileName string) (*Message, error){
    //fileName = "/root/go/docker/busybox.tar"
     mes := new(Message)
     mes.Type = "load"
     this = & Image{
         Message : mes,
     }
    tar, err := os.Open(fileName)
    if err != nil {
        mes.Mes = "open file error"
        return mes, err
    }
    defer tar.Close()
    err = client.LoadImage(docker.LoadImageOptions{InputStream: tar})
    if err != nil {
        mes.Mes = "load registry error"
        return mes, err
    } else {
        mes.Mes = "load registry success"
        mes.Code = 200 //执行成功
        return mes, nil
    }
}

//Tag iamge
func (this *Image)TagImage(client * docker.Client, image * Image) (*Message, error){
    //oldImage string, newImage string, newImageTag string
    oldImage := fmt.Sprintf("%s:%s", image.OldName, image.OldTag)
    //oldImage = "busybox:latest"
    newImage := fmt.Sprintf("%s:%s", image.Name, image.Tag)
    //newImage = "registry.cn-beijing.aliyuncs.com/hiacloud/busybox"
    newImageTag := image.Tag
    //newImageTag = "1.29.1"
    mes := new(Message)
    mes.Type = "tag"
/*    this = &Image{
        Tag:  "latest",
        Name: "registry.cn-beijing.aliyuncs.com/hiacloud/busybox",
        Project: "hiacloud",
        Registry: "registry.cn-beijing.aliyuncs.com",
        Message:  mes,
    }*/
    err := client.TagImage(oldImage, docker.TagImageOptions{
        Repo: newImage,
        Tag: newImageTag,
    })
    if err != nil && !strings.Contains(err.Error(), "tag registry fail") {
        mes.Mes = "tag registry error"
        return mes, err
    }else {
        mes.Mes = "tag registry success"
        mes.Code = 200 //执行成功
        return mes, nil
    }
}
// push registry
func (this *Image)PushImageCustomRegistry(client * docker.Client, image *Image) (*Message, error){
    mes := new(Message)
    mes.Type = "push"
    var buf bytes.Buffer
    registryAuth := docker.AuthConfiguration{
        Username: image.Registry.Username,
        Password: image.Registry.Password,
        ServerAddress: image.Registry.ServerAddress,
    }
    opts := docker.PushImageOptions{
        Name: image.Name,
        Tag: image.Tag,
        Registry: image.Registry.ServerAddress,
        RawJSONStream: true,
        OutputStream: &buf,
    }
   if err := client.PushImage(opts, registryAuth); err != nil {
       mes.Mes = "push registry error"
       return mes, err
   }else {
       mes.Mes = "push registry success"
       return mes, nil
   }
}
