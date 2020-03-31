package upload

type Image struct {
    Name string
    Tag string
    OldName string
    OldTag string
    Message *Message
    Registry *ImageRegistry
}

type ImageRegistry struct {
    Username string
    Password string
    ServerAddress string
}
type Message struct {
    Mes string
    Type string
    Code int
}



