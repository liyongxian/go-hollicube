package main
import (
    "bytes"
    "fmt"
    "os"
    "regexp"
    "strings"
    docker "go-client/hollicube-go/pkg/image/docker"
)

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
    tagImage(client)
    loadImage(client)
    pushImageCustomRegistry(client)

}

// load image
func loadImage(client * docker.Client) Message{
    var mes Message
    mes.Type = "load"
    tar, err := os.Open("/root/go/docker/busybox.tar")
    if err != nil {
        mes.Mes = "open file error"
        return mes
    }
    defer tar.Close()
    if client.LoadImage(docker.LoadImageOptions{InputStream: tar}) != nil {
        mes.Mes = "load image error"
        return mes
    } else {
        mes.Mes = "load image success"
        mes.Code = 200 //执行成功
        return mes
    }
}
//Tag iamge
func tagImage(client * docker.Client) Message{
    var mes Message
    mes.Type = "tag"
    err := client.TagImage("busybox:latest", docker.TagImageOptions{
        Repo: "registry.cn-beijing.aliyuncs.com/hiacloud/busybox",
        Tag: "1.29.1",
    })
    if err != nil && !strings.Contains(err.Error(), "tag image fail") {
        mes.Mes = "tag image error"
        return mes
    }else {
        mes.Mes = "tag image success"
        mes.Code = 200 //执行成功
        return mes
    }
}

// push image
func pushImageCustomRegistry(client * docker.Client) Message{
    var mes Message
    mes.Type = "push"
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
        OutputStream: &buf,
    }
    if err := client.PushImage(opts, registryAuth); err != nil {
        mes.Mes = "push image error"
        return mes
    }else {
        mes.Mes = "push image success"
        return mes
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
