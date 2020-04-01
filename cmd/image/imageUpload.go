package main
import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/astaxie/beego/httplib"
	mapset "github.com/deckarep/golang-set"
	jsoniter "github.com/json-iterator/go"
	"github.com/liyongxian/go-hollicube/pkg/registry/docker"
	"github.com/liyongxian/go-hollicube/pkg/registry/upload"
	"github.com/liyongxian/go-hollicube/pkg/registry/utils"
	"github.com/nfnt/resize"
	"github.com/radovskyb/watcher"
	"github.com/sjqzhang/goutil"
	log "github.com/sjqzhang/seelog"
	"github.com/sjqzhang/tusd"
	"github.com/sjqzhang/tusd/filestore"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"io/ioutil"
	slog "log"
	"mime/multipart"
	"net/http"
	_ "net/http/pprof"
	"net/smtp"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var staticHandler http.Handler
var json = jsoniter.ConfigCompatibleWithStandardLibrary
var server *Server
var logacc log.LoggerInterface
var FOLDERS = []string{DATA_DIR, STORE_DIR, CONF_DIR, STATIC_DIR}
var CONST_QUEUE_SIZE = 10000

var (
	VERSION     string
	BUILD_TIME  string
	GO_VERSION  string
	GIT_VERSION string
	v           = flag.Bool("v", false, "display version")
)
var (
	FileName                    string
	ptr                         unsafe.Pointer
	DOCKER_DIR                  = ""
	STORE_DIR                   = STORE_DIR_NAME
	CONF_DIR                    = CONF_DIR_NAME
	LOG_DIR                     = LOG_DIR_NAME
	DATA_DIR                    = DATA_DIR_NAME
	STATIC_DIR                  = STATIC_DIR_NAME
	LARGE_DIR_NAME              = "haystack"
	LARGE_DIR                   = STORE_DIR + "/haystack"
	CONST_LEVELDB_FILE_NAME     = DATA_DIR + "/fileserver.db"
	CONST_LOG_LEVELDB_FILE_NAME = DATA_DIR + "/log.db"
	CONST_STAT_FILE_NAME        = DATA_DIR + "/stat.json"
	CONST_CONF_FILE_NAME        = CONF_DIR + "/cfg.json"
	CONST_UPLOAD_COUNTER_KEY    = "__CONST_UPLOAD_COUNTER_KEY__"
	logConfigStr                = `
<seelog type="asynctimer" asyncinterval="1000" minlevel="trace" maxlevel="error">  
	<outputs formatid="common">  
		<buffered formatid="common" size="1048576" flushperiod="1000">  
			<rollingfile type="size" filename="{DOCKER_DIR}log/fileserver.log" maxsize="104857600" maxrolls="10"/>  
		</buffered>
	</outputs>  	  
	 <formats>
		 <format id="common" format="%Date %Time [%LEV] [%File:%Line] [%Func] %Msg%n" />  
	 </formats>  
</seelog>
`
	logAccessConfigStr = `
<seelog type="asynctimer" asyncinterval="1000" minlevel="trace" maxlevel="error">  
	<outputs formatid="common">  
		<buffered formatid="common" size="1048576" flushperiod="1000">  
			<rollingfile type="size" filename="{DOCKER_DIR}log/access.log" maxsize="104857600" maxrolls="10"/>  
		</buffered>
	</outputs>  	  
	 <formats>
		 <format id="common" format="%Date %Time [%LEV] [%File:%Line] [%Func] %Msg%n" />  
	 </formats>  
</seelog>
`
)

const (
	STORE_DIR_NAME                 = "files"
	LOG_DIR_NAME                   = "log"
	DATA_DIR_NAME                  = "data"
	CONF_DIR_NAME                  = "conf"
	STATIC_DIR_NAME                = "static"
	CONST_STAT_FILE_COUNT_KEY      = "fileCount"
	CONST_BIG_UPLOAD_PATH_SUFFIX   = "/file/upload/"
	CONST_STAT_FILE_TOTAL_SIZE_KEY = "totalSize"
	CONST_Md5_ERROR_FILE_NAME      = "errors.md5"
	CONST_Md5_QUEUE_FILE_NAME      = "queue.md5"
	CONST_FILE_Md5_FILE_NAME       = "files.md5"
	CONST_REMOME_Md5_FILE_NAME     = "removes.md5"
	CONST_SMALL_FILE_SIZE          = 1024 * 1024
	CONST_MESSAGE_CLUSTER_IP       = "Can only be called by the cluster ip or 127.0.0.1 or admin_ips(cfg.json),current ip:%s"
	cfgJson                        = `{
	"绑定端号": "端口",
	"addr": ":8080",
	"本主机地址": "本机http地址,默认自动生成(注意端口必须与addr中的端口一致），必段为内网，自动生成不为内网请自行修改，下同",
	"host": "%s",
	"集群": "集群列表,注意为了高可用，IP必须不能是同一个,同一不会自动备份，且不能为127.0.0.1,且必须为内网IP，默认自动生成",
	"peers": ["%s"],
	"组号": "用于区别不同的集群(上传或下载)与support_group_manage配合使用,带在下载路径中",
	"group": "hollicube",
	"是否支持按组（集群）管理,主要用途是Nginx支持多集群": "默认支持,不支持时路径为http://10.1.5.4:8080/action,支持时为http://10.1.5.4:8080/group(配置中的group参数)/action,action为动作名，如status,delete,sync等",
	"support_group_manage": true,
	"是否合并小文件": "默认不合并,合并可以解决inode不够用的情况（当前对于小于1M文件）进行合并",
	"enable_merge_small_file": false,
    "允许后缀名": "允许可以上传的文件后缀名，如jpg,jpeg,png等。留空允许所有。",
	"extensions": [],
	"重试同步失败文件的时间": "单位秒",
	"refresh_interval": 1800,
	"是否自动重命名": "默认不自动重命名,使用原文件名",
	"rename_file": false,
	"是否支持web上传,方便调试": "默认支持web上传",
	"enable_web_upload": true,
	"是否支持非日期路径": "默认支持非日期路径,也即支持自定义路径,需要上传文件时指定path",
	"enable_custom_path": true,
	"下载域名": "用于外网下载文件的域名,不包含http://",
	"download_domain": "",
	"场景列表": "当设定后，用户指的场景必项在列表中，默认不做限制(注意：如果想开启场景认功能，格式如下：'场景名:googleauth_secret' 如 default:N7IET373HB2C5M6D ",
	"scenes": [],
	"默认场景": "默认default",
	"default_scene": "upfiles",
	"是否显示目录": "默认显示,方便调试用,上线时请关闭",
	"show_dir": true,
	"邮件配置": "",
	"mail": {
		"rbac": "abc@163.com",
		"password": "abc",
		"host": "smtp.163.com:25"
	},
	"告警接收邮件列表": "接收人数组",
	"alarm_receivers": [],
	"告警接收URL": "方法post,参数:subject,message",
	"alarm_url": "",
	"下载是否需带token": "真假",
	"download_use_token": false,
	"下载token过期时间": "单位秒",
	"download_token_expire": 600,
	"文件去重算法md5可能存在冲突，默认md5": "sha1|md5",
	"file_sum_arithmetic": "md5",
	"管理ip列表": "用于管理集的ip白名单,",
	"admin_ips": ["127.0.0.1"],
	"是否启用迁移": "默认不启用",
	"enable_migrate": false,
	"文件是否去重": "默认去重",
	"enable_distinct_file": true,
	"是否开启Google认证，实现安全的上传、下载": "默认不开启",
	"enable_google_auth": false,
	"认证url": "当url不为空时生效,注意:普通上传中使用http参数 auth_token 作为认证参数, 在断点续传中通过HTTP头Upload-Metadata中的auth_token作为认证参数,认证流程参考认证架构图",
	"auth_url": "",
	"默认是否下载": "默认下载",
	"default_download": true,
	"本机是否只读": "默认可读可写",
	"read_only": false,
	"是否开启断点续传": "默认开启",
	"enable_tus": true,
	"同步单一文件超时时间（单位秒）": "默认为0,程序自动计算，在特殊情况下，自已设定",
	"sync_timeout": 0
}
	`
)

type Server struct {
	ldb            *leveldb.DB
	logDB          *leveldb.DB
	util           *goutil.Common
	statMap        *goutil.CommonMap
	sumMap         *goutil.CommonMap
	rtMap          *goutil.CommonMap
	queueToPeers   chan FileInfo
	queueFromPeers chan FileInfo
	queueFileLog   chan *FileLog
	queueUpload    chan WrapReqResp
	lockMap        *goutil.CommonMap
	sceneMap       *goutil.CommonMap
	searchMap      *goutil.CommonMap
	curDate        string
	host           string
}
type FileInfo struct {
	Name      string   `json:"name"`
	ReName    string   `json:"rename"`
	Path      string   `json:"path"`
	Md5       string   `json:"md5"`
	Size      int64    `json:"size"`
	Peers     []string `json:"peers"`
	Scene     string   `json:"scene"`
	TimeStamp int64    `json:"timeStamp"`
	OffSet    int64    `json:"offset"`
	retry     int
	op        string
}
type FileLog struct {
	FileInfo *FileInfo
	FileName string
}
type WrapReqResp struct {
	w    *http.ResponseWriter
	r    *http.Request
	done chan bool
}
type JsonResult struct {
	Message string      `json:"message"`
	Status  string      `json:"status"`
	Data    interface{} `json:"data"`
}
type FileResult struct {
	Url     string `json:"url"`
	Md5     string `json:"md5"`
	Path    string `json:"path"`
	Domain  string `json:"domain"`
	Scene   string `json:"scene"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mtime"`
	//Just for Compatibility
	Scenes  string `json:"scenes"`
	Retmsg  string `json:"retmsg"`
	Retcode int    `json:"retcode"`
	Src     string `json:"src"`
}
type Mail struct {
	User     string `json:"rbac"`
	Password string `json:"password"`
	Host     string `json:"host"`
}
type StatDateFileInfo struct {
	Date      string `json:"date"`
	TotalSize int64  `json:"totalSize"`
	FileCount int64  `json:"fileCount"`
}
type GloablConfig struct {
	Addr                 string   `json:"addr"`
	Peers                []string `json:"peers"`
	Group                string   `json:"group"`
	RenameFile           bool     `json:"rename_file"`
	ShowDir              bool     `json:"show_dir"`
	Extensions           []string `json:"extensions"`
	RefreshInterval      int      `json:"refresh_interval"`
	EnableWebUpload      bool     `json:"enable_web_upload"`
	DownloadDomain       string   `json:"download_domain"`
	EnableCustomPath     bool     `json:"enable_custom_path"`
	Scenes               []string `json:"scenes"`
	AlarmReceivers       []string `json:"alarm_receivers"`
	DefaultScene         string   `json:"default_scene"`
	Mail                 Mail     `json:"mail"`
	AlarmUrl             string   `json:"alarm_url"`
	DownloadUseToken     bool     `json:"download_use_token"`
	DownloadTokenExpire  int      `json:"download_token_expire"`
	QueueSize            int      `json:"queue_size"`
	Host                 string   `json:"host"`
	FileSumArithmetic    string   `json:"file_sum_arithmetic"`
	SupportGroupManage   bool     `json:"support_group_manage"`
	AdminIps             []string `json:"admin_ips"`
	EnableMergeSmallFile bool     `json:"enable_merge_small_file"`
	EnableMigrate        bool     `json:"enable_migrate"`
	EnableDistinctFile   bool     `json:"enable_distinct_file"`
	ReadOnly             bool     `json:"read_only"`
	EnableGoogleAuth     bool     `json:"enable_google_auth"`
	AuthUrl              string   `json:"auth_url"`
	DefaultDownload      bool     `json:"default_download"`
	EnableTus            bool     `json:"enable_tus"`
	SyncTimeout          int64    `json:"sync_timeout"`
	EnableFsnotify       bool     `json:"enable_fsnotify"`
	EnableDiskCache      bool     `json:"enable_disk_cache"`
	ConnectTimeout       bool     `json:"connect_timeout"`
	ReadTimeout          int      `json:"read_timeout"`
	WriteTimeout         int      `json:"write_timeout"`
	IdleTimeout          int      `json:"idle_timeout"`
	ReadHeaderTimeout    int      `json:"read_header_timeout"`
	SyncWorker           int      `json:"sync_worker"`
	UploadWorker         int      `json:"upload_worker"`
	UploadQueueSize      int      `json:"upload_queue_size"`
	RetryCount           int      `json:"retry_count"`
	SyncDelay            int64    `json:"sync_delay"`
	WatchChanSize        int      `json:"watch_chan_size"`
}
type FileInfoResult struct {
	Name    string `json:"name"`
	Md5     string `json:"md5"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mtime"`
	IsDir   bool   `json:"is_dir"`
}

func NewServer() *Server {
	var (
		server *Server
		err    error
	)
	server = &Server{
		util:           &goutil.Common{},
		statMap:        goutil.NewCommonMap(0),
		lockMap:        goutil.NewCommonMap(0),
		rtMap:          goutil.NewCommonMap(0),
		sceneMap:       goutil.NewCommonMap(0),
		searchMap:      goutil.NewCommonMap(0),
		queueToPeers:   make(chan FileInfo, CONST_QUEUE_SIZE),
		queueFromPeers: make(chan FileInfo, CONST_QUEUE_SIZE),
		queueFileLog:   make(chan *FileLog, CONST_QUEUE_SIZE),
		queueUpload:    make(chan WrapReqResp, 100),
		sumMap:         goutil.NewCommonMap(365 * 3),
	}

	defaultTransport := &http.Transport{
		DisableKeepAlives:   true,
		Dial:                httplib.TimeoutDialer(time.Second*15, time.Second*300),
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
	}
	settins := httplib.BeegoHTTPSettings{
		UserAgent:        "Go-FastDFS",
		ConnectTimeout:   15 * time.Second,
		ReadWriteTimeout: 15 * time.Second,
		Gzip:             true,
		DumpBody:         true,
		Transport:        defaultTransport,
	}
	httplib.SetDefaultSetting(settins)
	server.statMap.Put(CONST_STAT_FILE_COUNT_KEY, int64(0))
	server.statMap.Put(CONST_STAT_FILE_TOTAL_SIZE_KEY, int64(0))
	server.statMap.Put(server.util.GetToDay()+"_"+CONST_STAT_FILE_COUNT_KEY, int64(0))
	server.statMap.Put(server.util.GetToDay()+"_"+CONST_STAT_FILE_TOTAL_SIZE_KEY, int64(0))
	server.curDate = server.util.GetToDay()
	opts := &opt.Options{
		CompactionTableSize: 1024 * 1024 * 20,
		WriteBuffer:         1024 * 1024 * 20,
	}
	server.ldb, err = leveldb.OpenFile(CONST_LEVELDB_FILE_NAME, opts)
	if err != nil {
		fmt.Println(fmt.Sprintf("open db file %s fail,maybe has opening", CONST_LEVELDB_FILE_NAME))
		log.Error(err)
		panic(err)
	}
	server.logDB, err = leveldb.OpenFile(CONST_LOG_LEVELDB_FILE_NAME, opts)
	if err != nil {
		fmt.Println(fmt.Sprintf("open db file %s fail,maybe has opening", CONST_LOG_LEVELDB_FILE_NAME))
		log.Error(err)
		panic(err)

	}
	return server
}

func Config() *GloablConfig {
	return (*GloablConfig)(atomic.LoadPointer(&ptr))
}
func ParseConfig(filePath string) {
	var (
		data []byte
	)
	if filePath == "" {
		data = []byte(strings.TrimSpace(cfgJson))
	} else {
		file, err := os.Open(filePath)
		if err != nil {
			panic(fmt.Sprintln("open file path:", filePath, "error:", err))
		}
		defer file.Close()
		FileName = filePath
		data, err = ioutil.ReadAll(file)
		if err != nil {
			panic(fmt.Sprintln("file path:", filePath, " read all error:", err))
		}
	}
	var c GloablConfig
	if err := json.Unmarshal(data, &c); err != nil {
		panic(fmt.Sprintln("file path:", filePath, "json unmarshal error:", err))
	}
	log.Info(c)
	atomic.StorePointer(&ptr, unsafe.Pointer(&c))
	log.Info("config parse success")
}


func (this *Server) WatchFilesChange() {
	fmt.Println("WatchFilesChange func is called!")
	var (
		w *watcher.Watcher
		//fileInfo FileInfo
		curDir string
		err    error
		qchan  chan *FileInfo
		isLink bool
	)
	qchan = make(chan *FileInfo, Config().WatchChanSize)
	w = watcher.New()
	w.FilterOps(watcher.Create)
	//w.FilterOps(watcher.Create, watcher.Remove)
	curDir, err = filepath.Abs(filepath.Dir(STORE_DIR_NAME))
	if err != nil {
		log.Error(err)
	}
	go func() {
		for {
			select {
			case event := <-w.Event:
				if event.IsDir() {
					continue
				}

				fpath := strings.Replace(event.Path, curDir+string(os.PathSeparator), "", 1)
				if isLink {
					fpath = strings.Replace(event.Path, curDir, STORE_DIR_NAME, 1)
				}
				fpath = strings.Replace(fpath, string(os.PathSeparator), "/", -1)
				sum := this.util.MD5(fpath)
				fileInfo := FileInfo{
					Size:      event.Size(),
					Name:      event.Name(),
					Path:      strings.TrimSuffix(fpath, "/"+event.Name()), // files/default/20190927/xxx
					Md5:       sum,
					TimeStamp: event.ModTime().Unix(),
					Peers:     []string{this.host},
					OffSet:    -2,
					op:        event.Op.String(),
				}
				log.Info(fmt.Sprintf("WatchFilesChange op:%s path:%s", event.Op.String(), fpath))
				qchan <- &fileInfo
				//this.AppendToQueue(&fileInfo)
			case err := <-w.Error:
				log.Error(err)
			case <-w.Closed:
				return
			}
		}
	}()
	go func() {
		for {
			c := <-qchan
			if time.Now().Unix()-c.TimeStamp < Config().SyncDelay {
				qchan <- c
				time.Sleep(time.Second * 1)
				continue
			} else {
				//if c.op == watcher.Remove.String() {
				//	req := httplib.Post(fmt.Sprintf("%s%s?md5=%s", this.host, this.getRequestURI("delete"), c.Md5))
				//	req.Param("md5", c.Md5)
				//	req.SetTimeout(time.Second*5, time.Second*10)
				//	log.Infof(req.String())
				//}

				if c.op == watcher.Create.String() {
					log.Info(fmt.Sprintf("Syncfile Add to Queue path:%s", c.Path+"/"+c.Name))
					this.AppendToQueue(c)
					this.SaveFileInfoToLevelDB(c.Md5, c, this.ldb)
				}
			}
		}
	}()
	if dir, err := os.Readlink(STORE_DIR_NAME); err == nil {

		if strings.HasSuffix(dir, string(os.PathSeparator)) {
			dir = strings.TrimSuffix(dir, string(os.PathSeparator))
		}
		curDir = dir
		isLink = true
		if err := w.AddRecursive(dir); err != nil {
			log.Error(err)
		}
		w.Ignore(dir + "/_tmp/")
		w.Ignore(dir + "/" + LARGE_DIR_NAME + "/")
	}
	if err := w.AddRecursive("./" + STORE_DIR_NAME); err != nil {
		log.Error(err)
	}
	w.Ignore("./" + STORE_DIR_NAME + "/_tmp/")
	w.Ignore("./" + STORE_DIR_NAME + "/" + LARGE_DIR_NAME + "/")
	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Error(err)
	}
}

func (this *Server) GetFilePathByInfo(fileInfo *FileInfo, withDocker bool) string {
	fmt.Println("GetFilePathByInfo func is called!")
	var (
		fn string
	)
	fn = fileInfo.Name
	if fileInfo.ReName != "" {
		fn = fileInfo.ReName
	}
	if withDocker {
		return DOCKER_DIR + fileInfo.Path + "/" + fn
	}
	fmt.Printf("fileInfo.Path is: %v%v",fileInfo.Path, fn)
	return fileInfo.Path + "/" + fn
}
func (this *Server) CheckFileExistByInfo(md5s string, fileInfo *FileInfo) bool {
	fmt.Println("CheckFileExistByInfo func is called!")
	var (
		err      error
		fullpath string
		fi       os.FileInfo
		info     *FileInfo
	)
	if fileInfo == nil {
		return false
	}
	if fileInfo.OffSet >= 0 {
		//small file
		if info, err = this.GetFileInfoFromLevelDB(fileInfo.Md5); err == nil && info.Md5 == fileInfo.Md5 {
			return true
		} else {
			return false
		}
	}
	fullpath = this.GetFilePathByInfo(fileInfo, true)
	if fi, err = os.Stat(fullpath); err != nil {
		return false
	}
	if fi.Size() == fileInfo.Size {
		return true
	} else {
		return false
	}
}
func (this *Server) ParseSmallFile(filename string) (string, int64, int, error) {
	fmt.Println("ParseSmallFile func is called!")
	var (
		err    error
		offset int64
		length int
	)
	err = errors.New("unvalid small file")
	if len(filename) < 3 {
		return filename, -1, -1, err
	}
	if strings.Contains(filename, "/") {
		filename = filename[strings.LastIndex(filename, "/")+1:]
	}
	pos := strings.Split(filename, ",")
	if len(pos) < 3 {
		return filename, -1, -1, err
	}
	offset, err = strconv.ParseInt(pos[1], 10, 64)
	if err != nil {
		return filename, -1, -1, err
	}
	if length, err = strconv.Atoi(pos[2]); err != nil {
		return filename, offset, -1, err
	}
	if length > CONST_SMALL_FILE_SIZE || offset < 0 {
		err = errors.New("invalid filesize or offset")
		return filename, -1, -1, err
	}
	return pos[0], offset, length, nil
}
func (this *Server) DownloadFromPeer(peer string, fileInfo *FileInfo) {
	fmt.Println("DownloadFromPeer func is called!")
	var (
		err         error
		filename    string
		fpath       string
		fpathTmp    string
		fi          os.FileInfo
		data        []byte
		downloadUrl string
	)
	if Config().ReadOnly {
		log.Warn("ReadOnly", fileInfo)
		return
	}
	if Config().RetryCount > 0 && fileInfo.retry >= Config().RetryCount {
		log.Error("DownloadFromPeer Error ", fileInfo)
		return
	} else {
		fileInfo.retry = fileInfo.retry + 1
	}
	filename = fileInfo.Name
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	if fileInfo.OffSet != -2 && Config().EnableDistinctFile && this.CheckFileExistByInfo(fileInfo.Md5, fileInfo) {
		// ignore migrate file
		log.Info(fmt.Sprintf("DownloadFromPeer file Exist, path:%s", fileInfo.Path+"/"+fileInfo.Name))
		return
	}
	if (!Config().EnableDistinctFile || fileInfo.OffSet == -2) && this.util.FileExists(this.GetFilePathByInfo(fileInfo, true)) {
		// ignore migrate file
		if fi, err = os.Stat(this.GetFilePathByInfo(fileInfo, true)); err == nil {
			if fi.ModTime().Unix() > fileInfo.TimeStamp {
				log.Info(fmt.Sprintf("ignore file sync path:%s", this.GetFilePathByInfo(fileInfo, false)))
				fileInfo.TimeStamp = fi.ModTime().Unix()
				this.postFileToPeer(fileInfo) // keep newer
				return
			}
			os.Remove(this.GetFilePathByInfo(fileInfo, true))
		}
	}
	if _, err = os.Stat(fileInfo.Path); err != nil {
		os.MkdirAll(DOCKER_DIR+fileInfo.Path, 0775)
	}
	fmt.Println("downloadFromPeer  fileInfo ",fileInfo)
	p := strings.Replace(fileInfo.Path, STORE_DIR_NAME+"/", "", 1)
	//filename=this.util.UrlEncode(filename)
	downloadUrl = peer + "/" + Config().Group + "/" + p + "/" + filename
	fmt.Println("downloadFromPeer  downloadUrl",downloadUrl)
	log.Info("DownloadFromPeer: ", downloadUrl)
	fpath = DOCKER_DIR + fileInfo.Path + "/" + filename
	fpathTmp = DOCKER_DIR + fileInfo.Path + "/" + fmt.Sprintf("%s_%s", "tmp_", filename)
	timeout := fileInfo.Size/1024/1024/1 + 30
	if Config().SyncTimeout > 0 {
		timeout = Config().SyncTimeout
	}
	this.lockMap.LockKey(fpath)
	defer this.lockMap.UnLockKey(fpath)
	download_key := fmt.Sprintf("downloading_%d_%s", time.Now().Unix(), fpath)
	this.ldb.Put([]byte(download_key), []byte(""), nil)
	defer func() {
		this.ldb.Delete([]byte(download_key), nil)
	}()
	if fileInfo.OffSet == -2 {
		//migrate file
		if fi, err = os.Stat(fpath); err == nil && fi.Size() == fileInfo.Size {
			//prevent double download
			this.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, this.ldb)
			//log.Info(fmt.Sprintf("file '%s' has download", fpath))
			return
		}
		req := httplib.Get(downloadUrl)
		req.SetTimeout(time.Second*30, time.Second*time.Duration(timeout))
		if err = req.ToFile(fpathTmp); err != nil {
			this.AppendToDownloadQueue(fileInfo) //retry
			os.Remove(fpathTmp)
			log.Error(err, fpathTmp)
			return
		}
		if os.Rename(fpathTmp, fpath) == nil {
			//this.SaveFileMd5Log(fileInfo, CONST_FILE_Md5_FILE_NAME)
			this.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, this.ldb)
		}
		return
	}
	req := httplib.Get(downloadUrl)
	req.SetTimeout(time.Second*30, time.Second*time.Duration(timeout))
	if fileInfo.OffSet >= 0 {
		//small file download
		data, err = req.Bytes()
		if err != nil {
			this.AppendToDownloadQueue(fileInfo) //retry
			log.Error(err)
			return
		}
		data2 := make([]byte, len(data)+1)
		data2[0] = '1'
		for i, v := range data {
			data2[i+1] = v
		}
		data = data2
		if int64(len(data)) != fileInfo.Size {
			log.Warn("file size is error")
			return
		}
		fpath = strings.Split(fpath, ",")[0]
		err = this.util.WriteFileByOffSet(fpath, fileInfo.OffSet, data)
		if err != nil {
			log.Warn(err)
			return
		}
		this.SaveFileMd5Log(fileInfo, CONST_FILE_Md5_FILE_NAME)
		fmt.Println("1111111111  upload file success!!!")
		return
	}
	if err = req.ToFile(fpathTmp); err != nil {
		this.AppendToDownloadQueue(fileInfo) //retry
		os.Remove(fpathTmp)
		log.Error(err)
		return
	}
	if fi, err = os.Stat(fpathTmp); err != nil {
		os.Remove(fpathTmp)
		fmt.Println("22222222222  upload file success!!!")
		return
	}

	if fi.Size() != fileInfo.Size { //  maybe has bug remove || sum != fileInfo.Md5
		log.Error("file sum check error")
		os.Remove(fpathTmp)
		return
	}
	if os.Rename(fpathTmp, fpath) == nil {
		this.SaveFileMd5Log(fileInfo, CONST_FILE_Md5_FILE_NAME)
	}
}

func (this *Server) SetDownloadHeader(w http.ResponseWriter, r *http.Request) {
	fmt.Println("SetDownloadHeader func is called!")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment")
}

func (this *Server) NotPermit(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(401)
}

func (this *Server) GetFilePathFromRequest(w http.ResponseWriter, r *http.Request) (string, string) {
	fmt.Println("GetFilePathFromRequest func is called!")
	fmt.Println("GetFilePathFromRequest is called")
	var (
		err       error
		fullpath  string
		smallPath string
		prefix    string
	)
	fullpath = r.RequestURI[1:]
	if strings.HasPrefix(r.RequestURI, "/"+Config().Group+"/") {
		fullpath = r.RequestURI[len(Config().Group)+2 : len(r.RequestURI)]
	}
	fullpath = strings.Split(fullpath, "?")[0] // just path
	fullpath = DOCKER_DIR + STORE_DIR_NAME + "/" + fullpath
	prefix = "/" + LARGE_DIR_NAME + "/"
	if Config().SupportGroupManage {
		prefix = "/" + Config().Group + "/" + LARGE_DIR_NAME + "/"
	}
	if strings.HasPrefix(r.RequestURI, prefix) {
		smallPath = fullpath //notice order
		fullpath = strings.Split(fullpath, ",")[0]
	}
	if fullpath, err = url.PathUnescape(fullpath); err != nil {
		log.Error(err)
	}
	return fullpath, smallPath
}

func (this *Server) GetSmallFileByURI(w http.ResponseWriter, r *http.Request) ([]byte, bool, error) {
	fmt.Println("GetSmallFileByURI func is called!")
	fmt.Println("GetSmallFileByURI func is called")
	var (
		err      error
		data     []byte
		offset   int64
		length   int
		fullpath string
		info     os.FileInfo
	)
	fullpath, _ = this.GetFilePathFromRequest(w, r)
	if _, offset, length, err = this.ParseSmallFile(r.RequestURI); err != nil {
		return nil, false, err
	}
	if info, err = os.Stat(fullpath); err != nil {
		return nil, false, err
	}
	if info.Size() < offset+int64(length) {
		return nil, true, errors.New("noFound")
	} else {
		data, err = this.util.ReadFileByOffSet(fullpath, offset, length)
		if err != nil {
			return nil, false, err
		}
		return data, false, err
	}
}
func (this *Server) DownloadSmallFileByURI(w http.ResponseWriter, r *http.Request) (bool, error) {
	fmt.Println("DownloadSmallFileByURI func is called!")
	var (
		err        error
		data       []byte
		isDownload bool
		imgWidth   int
		imgHeight  int
		width      string
		height     string
		notFound   bool
	)
	r.ParseForm()
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	width = r.FormValue("width")
	height = r.FormValue("height")
	if width != "" {
		imgWidth, err = strconv.Atoi(width)
		if err != nil {
			log.Error(err)
		}
	}
	if height != "" {
		imgHeight, err = strconv.Atoi(height)
		if err != nil {
			log.Error(err)
		}
	}
	data, notFound, err = this.GetSmallFileByURI(w, r)
	_ = notFound
	if data != nil && string(data[0]) == "1" {
		if isDownload {
			this.SetDownloadHeader(w, r)
		}
		if imgWidth != 0 || imgHeight != 0 {
			this.ResizeImageByBytes(w, data[1:], uint(imgWidth), uint(imgHeight))
			return true, nil
		}
		w.Write(data[1:])
		return true, nil
	}
	return false, errors.New("not found")
}
func (this *Server) DownloadNormalFileByURI(w http.ResponseWriter, r *http.Request) (bool, error) {
	fmt.Println("DownloadNormalFileByURI func is called!")
	var (
		err        error
		isDownload bool
		imgWidth   int
		imgHeight  int
		width      string
		height     string
	)
	r.ParseForm()
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	width = r.FormValue("width")
	height = r.FormValue("height")
	if width != "" {
		imgWidth, err = strconv.Atoi(width)
		if err != nil {
			log.Error(err)
		}
	}
	if height != "" {
		imgHeight, err = strconv.Atoi(height)
		if err != nil {
			log.Error(err)
		}
	}
	if isDownload {
		this.SetDownloadHeader(w, r)
	}
	fullpath, _ := this.GetFilePathFromRequest(w, r)
	if imgWidth != 0 || imgHeight != 0 {
		this.ResizeImage(w, fullpath, uint(imgWidth), uint(imgHeight))
		return true, nil
	}
	staticHandler.ServeHTTP(w, r)
	return true, nil
}
func (this *Server) DownloadNotFound(w http.ResponseWriter, r *http.Request) {
	fmt.Println("DownloadNotFound func is called!")
	var (
		err        error
		fullpath   string
		smallPath  string
		isDownload bool
		pathMd5    string
		peer       string
		fileInfo   *FileInfo
	)
	fullpath, smallPath = this.GetFilePathFromRequest(w, r)
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	if smallPath != "" {
		pathMd5 = this.util.MD5(smallPath)
	} else {
		pathMd5 = this.util.MD5(fullpath)
	}
	for _, peer = range Config().Peers {
		if fileInfo, err = this.checkPeerFileExist(peer, pathMd5, fullpath); err != nil {
			log.Error(err)
			continue
		}
		if fileInfo.Md5 != "" {
			go this.DownloadFromPeer(peer, fileInfo)
			//http.Redirect(w, r, peer+r.RequestURI, 302)
			if isDownload {
				this.SetDownloadHeader(w, r)
			}
			this.DownloadFileToResponse(peer+r.RequestURI, w, r)
			return
		}
	}
	w.WriteHeader(404)
	return
}
func (this *Server) Download(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Download func is called!")
	var (
		err       error
		ok        bool
		fullpath  string
		smallPath string
		fi        os.FileInfo
	)
	// redirect to upload
	if r.RequestURI == "/" || r.RequestURI == "" ||
		r.RequestURI == "/"+Config().Group ||
		r.RequestURI == "/"+Config().Group+"/" {
		this.Index(w, r)
		return
	}

	fullpath, smallPath = this.GetFilePathFromRequest(w, r)
	if smallPath == "" {
		if fi, err = os.Stat(fullpath); err != nil {
			this.DownloadNotFound(w, r)
			return
		}
		if !Config().ShowDir && fi.IsDir() {
			w.Write([]byte("list dir deny"))
			return
		}
		//staticHandler.ServeHTTP(w, r)
		this.DownloadNormalFileByURI(w, r)
		return
	}
	if smallPath != "" {
		if ok, err = this.DownloadSmallFileByURI(w, r); !ok {
			this.DownloadNotFound(w, r)
			return
		}
		return
	}

}
func (this *Server) DownloadFileToResponse(url string, w http.ResponseWriter, r *http.Request) {
	fmt.Println("DownloadFileToResponse func is called!")
	var (
		err  error
		req  *httplib.BeegoHTTPRequest
		resp *http.Response
	)
	req = httplib.Get(url)
	req.SetTimeout(time.Second*20, time.Second*600)
	resp, err = req.DoRequest()
	if err != nil {
		log.Error(err)
	}
	defer resp.Body.Close()
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Error(err)
	}
}
func (this *Server) ResizeImageByBytes(w http.ResponseWriter, data []byte, width, height uint) {
	fmt.Println("ResizeImageByBytes func is called!")
	var (
		img     image.Image
		err     error
		imgType string
	)
	reader := bytes.NewReader(data)
	img, imgType, err = image.Decode(reader)
	if err != nil {
		log.Error(err)
		return
	}
	img = resize.Resize(width, height, img, resize.Lanczos3)
	if imgType == "jpg" || imgType == "jpeg" {
		jpeg.Encode(w, img, nil)
	} else if imgType == "png" {
		png.Encode(w, img)
	} else {
		w.Write(data)
	}
}
func (this *Server) ResizeImage(w http.ResponseWriter, fullpath string, width, height uint) {
	fmt.Println("ResizeImage func is called")
	var (
		img     image.Image
		err     error
		imgType string
		file    *os.File
	)
	file, err = os.Open(fullpath)
	if err != nil {
		log.Error(err)
		return
	}
	img, imgType, err = image.Decode(file)
	if err != nil {
		log.Error(err)
		return
	}
	file.Close()
	img = resize.Resize(width, height, img, resize.Lanczos3)
	if imgType == "jpg" || imgType == "jpeg" {
		jpeg.Encode(w, img, nil)
	} else if imgType == "png" {
		png.Encode(w, img)
	} else {
		file.Seek(0, 0)
		io.Copy(w, file)
	}
}
func (this *Server) GetServerURI(r *http.Request) string {
	return fmt.Sprintf("http://%s/", r.Host)
}
func (this *Server) CheckFileAndSendToPeer(date string, filename string, isForceUpload bool) {
	fmt.Println("CheckFileAndSendToPeer func is called")
	var (
		md5set mapset.Set
		err    error
		md5s   []interface{}
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("CheckFileAndSendToPeer")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	if md5set, err = this.GetMd5sByDate(date, filename); err != nil {
		log.Error(err)
		return
	}
	md5s = md5set.ToSlice()
	for _, md := range md5s {
		if md == nil {
			continue
		}
		if fileInfo, _ := this.GetFileInfoFromLevelDB(md.(string)); fileInfo != nil && fileInfo.Md5 != "" {
			if isForceUpload {
				fileInfo.Peers = []string{}
			}
			if len(fileInfo.Peers) > len(Config().Peers) {
				continue
			}
			if !this.util.Contains(this.host, fileInfo.Peers) {
				fileInfo.Peers = append(fileInfo.Peers, this.host) // peer is null
			}
			if filename == CONST_Md5_QUEUE_FILE_NAME {
				this.AppendToDownloadQueue(fileInfo)
			} else {
				this.AppendToQueue(fileInfo)
			}
		}
	}
}
func (this *Server) postFileToPeer(fileInfo *FileInfo) {
	fmt.Println("postFileToPeer func is called")
	fmt.Println("postFileToPeer result is  fileInfo ",fileInfo.Path+"/"+fileInfo.Name)
	fmt.Println("postFileToPeer result is  fileInfo struct  ",fileInfo)
	var (
		err      error
		peer     string
		filename string
		info     *FileInfo
		postURL  string
		result   string
		fi       os.FileInfo
		i        int
		data     []byte
		fpath    string
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("postFileToPeer")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	//fmt.Println("postFile",fileInfo)
	for i, peer = range Config().Peers {
		_ = i
		if fileInfo.Peers == nil {
			fileInfo.Peers = []string{}
		}
		if this.util.Contains(peer, fileInfo.Peers) {
			continue
		}
		filename = fileInfo.Name
		if fileInfo.ReName != "" {
			filename = fileInfo.ReName
			if fileInfo.OffSet != -1 {
				filename = strings.Split(fileInfo.ReName, ",")[0]
			}
		}
		fpath = DOCKER_DIR + fileInfo.Path + "/" + filename
		fmt.Println("aaaaaaaaaaaaaaaaaaaaa fpath is ", fpath)
		if !this.util.FileExists(fpath) {
			log.Warn(fmt.Sprintf("file '%s' not found", fpath))
			continue
		} else {
			if fileInfo.Size == 0 {
				if fi, err = os.Stat(fpath); err != nil {
					log.Error(err)
				} else {
					fileInfo.Size = fi.Size()
				}
			}
		}
		if fileInfo.OffSet != -2 && Config().EnableDistinctFile {
			//not migrate file should check or update file
			// where not EnableDistinctFile should check
			if info, err = this.checkPeerFileExist(peer, fileInfo.Md5, ""); info.Md5 != "" {
				fileInfo.Peers = append(fileInfo.Peers, peer)
				if _, err = this.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, this.ldb); err != nil {
					log.Error(err)
				}
				continue
			}
		}
		postURL = fmt.Sprintf("%s%s", peer, this.getRequestURI("syncfile_info"))
		fmt.Println("bbbbbbbbbbbbb peer is", peer)
		fmt.Println("bbbbbbbbbbbbb postURL is", postURL)
		b := httplib.Post(postURL)
		b.SetTimeout(time.Second*30, time.Second*30)
		if data, err = json.Marshal(fileInfo); err != nil {
			log.Error(err)
			return
		}
		b.Param("fileInfo", string(data))
		result, err = b.String()
		if err != nil {
			if fileInfo.retry <= Config().RetryCount {
				fileInfo.retry = fileInfo.retry + 1
				this.AppendToQueue(fileInfo)
			}
			log.Error(err, fmt.Sprintf(" path:%s", fileInfo.Path+"/"+fileInfo.Name))
		}
		if !strings.HasPrefix(result, "http://") || err != nil {
			this.SaveFileMd5Log(fileInfo, CONST_Md5_ERROR_FILE_NAME)
		}
		if strings.HasPrefix(result, "http://") {
			log.Info(result)
			if !this.util.Contains(peer, fileInfo.Peers) {
				fileInfo.Peers = append(fileInfo.Peers, peer)
				if _, err = this.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, this.ldb); err != nil {
					log.Error(err)
				}
			}
		}
		if err != nil {
			log.Error(err)
		}
	}

}
func (this *Server) SaveFileMd5Log(fileInfo *FileInfo, filename string) {
	fmt.Println("SaveFileMd5Log func is called")
	var (
		info FileInfo
	)
	for len(this.queueFileLog)+len(this.queueFileLog)/10 > CONST_QUEUE_SIZE {
		time.Sleep(time.Second * 1)
	}
	info = *fileInfo
	this.queueFileLog <- &FileLog{FileInfo: &info, FileName: filename}
}
func (this *Server) saveFileMd5Log(fileInfo *FileInfo, filename string) {
	fmt.Println("saveFileMd5Log func is called")
	var (
		err      error
		outname  string
		logDate  string
		ok       bool
		fullpath string
		md5Path  string
		logKey   string
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("saveFileMd5Log")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	if fileInfo == nil || fileInfo.Md5 == "" || filename == "" {
		log.Warn("saveFileMd5Log", fileInfo, filename)
		return
	}
	logDate = this.util.GetDayFromTimeStamp(fileInfo.TimeStamp)
	outname = fileInfo.Name
	if fileInfo.ReName != "" {
		outname = fileInfo.ReName
	}
	fullpath = fileInfo.Path + "/" + outname
	logKey = fmt.Sprintf("%s_%s_%s", logDate, filename, fileInfo.Md5)
	if filename == CONST_FILE_Md5_FILE_NAME {
		//this.searchMap.Put(fileInfo.Md5, fileInfo.Name)
		if ok, err = this.IsExistFromLevelDB(fileInfo.Md5, this.ldb); !ok {
			this.statMap.AddCountInt64(logDate+"_"+CONST_STAT_FILE_COUNT_KEY, 1)
			this.statMap.AddCountInt64(logDate+"_"+CONST_STAT_FILE_TOTAL_SIZE_KEY, fileInfo.Size)
			this.SaveStat()
		}
		if _, err = this.SaveFileInfoToLevelDB(logKey, fileInfo, this.logDB); err != nil {
			log.Error(err)
		}
		if _, err := this.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, this.ldb); err != nil {
			log.Error("saveToLevelDB", err, fileInfo)
		}
		if _, err = this.SaveFileInfoToLevelDB(this.util.MD5(fullpath), fileInfo, this.ldb); err != nil {
			log.Error("saveToLevelDB", err, fileInfo)
		}
		return
	}
	if filename == CONST_REMOME_Md5_FILE_NAME {
		//this.searchMap.Remove(fileInfo.Md5)
		if ok, err = this.IsExistFromLevelDB(fileInfo.Md5, this.ldb); ok {
			this.statMap.AddCountInt64(logDate+"_"+CONST_STAT_FILE_COUNT_KEY, -1)
			this.statMap.AddCountInt64(logDate+"_"+CONST_STAT_FILE_TOTAL_SIZE_KEY, -fileInfo.Size)
			this.SaveStat()
		}
		this.RemoveKeyFromLevelDB(logKey, this.logDB)
		md5Path = this.util.MD5(fullpath)
		if err := this.RemoveKeyFromLevelDB(fileInfo.Md5, this.ldb); err != nil {
			log.Error("RemoveKeyFromLevelDB", err, fileInfo)
		}
		if err = this.RemoveKeyFromLevelDB(md5Path, this.ldb); err != nil {
			log.Error("RemoveKeyFromLevelDB", err, fileInfo)
		}
		// remove files.md5 for stat info(repair from logDB)
		logKey = fmt.Sprintf("%s_%s_%s", logDate, CONST_FILE_Md5_FILE_NAME, fileInfo.Md5)
		this.RemoveKeyFromLevelDB(logKey, this.logDB)
		return
	}
	this.SaveFileInfoToLevelDB(logKey, fileInfo, this.logDB)
}
func (this *Server) checkPeerFileExist(peer string, md5sum string, fpath string) (*FileInfo, error) {
	fmt.Println("checkPeerFileExist func is called")
	var (
		err      error
		fileInfo FileInfo
	)
	req := httplib.Post(fmt.Sprintf("%s%s?md5=%s", peer, this.getRequestURI("check_file_exist"), md5sum))
	req.Param("path", fpath)
	req.Param("md5", md5sum)
	req.SetTimeout(time.Second*5, time.Second*10)
	if err = req.ToJSON(&fileInfo); err != nil {
		return &FileInfo{}, err
	}
	if fileInfo.Md5 == "" {
		return &fileInfo, errors.New("not found")
	}
	return &fileInfo, nil
}


func (this *Server) IsExistFromLevelDB(key string, db *leveldb.DB) (bool, error) {
	fmt.Println("IsExistFromLevelDB func is called")
	return db.Has([]byte(key), nil)
}
func (this *Server) GetFileInfoFromLevelDB(key string) (*FileInfo, error) {
	fmt.Println("GetFileInfoFromLevelDB func is called")
	var (
		err      error
		data     []byte
		fileInfo FileInfo
	)
	if data, err = this.ldb.Get([]byte(key), nil); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, &fileInfo); err != nil {
		return nil, err
	}
	return &fileInfo, nil
}
func (this *Server) SaveStat() {
	fmt.Println("SaveStat func is called")
	SaveStatFunc := func() {
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				log.Error("SaveStatFunc")
				log.Error(re)
				log.Error(string(buffer))
			}
		}()
		stat := this.statMap.Get()
		if v, ok := stat[CONST_STAT_FILE_COUNT_KEY]; ok {
			switch v.(type) {
			case int64, int32, int, float64, float32:
				if v.(int64) >= 0 {
					if data, err := json.Marshal(stat); err != nil {
						log.Error(err)
					} else {
						this.util.WriteBinFile(CONST_STAT_FILE_NAME, data)
					}
				}
			}
		}
	}
	SaveStatFunc()
}
func (this *Server) RemoveKeyFromLevelDB(key string, db *leveldb.DB) error {
	fmt.Println("RemoveKeyFromLevelDB func is called")
	var (
		err error
	)
	err = db.Delete([]byte(key), nil)
	return err
}
func (this *Server) SaveFileInfoToLevelDB(key string, fileInfo *FileInfo, db *leveldb.DB) (*FileInfo, error) {
	fmt.Println("SaveFileInfoToLevelDB func is called")
	var (
		err  error
		data []byte
	)
	if fileInfo == nil || db == nil {
		return nil, errors.New("fileInfo is null or db is null")
	}
	if data, err = json.Marshal(fileInfo); err != nil {
		return fileInfo, err
	}
	if err = db.Put([]byte(key), data, nil); err != nil {
		return fileInfo, err
	}
	if db == this.ldb { //search slow ,write fast, double write logDB
		logDate := this.util.GetDayFromTimeStamp(fileInfo.TimeStamp)
		logKey := fmt.Sprintf("%s_%s_%s", logDate, CONST_FILE_Md5_FILE_NAME, fileInfo.Md5)
		this.logDB.Put([]byte(logKey), data, nil)
	}
	return fileInfo, nil
}
func (this *Server) IsPeer(r *http.Request) bool {
	fmt.Println("IsPeer func is called")
	var (
		ip    string
		peer  string
		bflag bool
	)
	//return true
	ip = this.util.GetClientIp(r)
	realIp := os.Getenv("GO_FASTDFS_IP")
	if realIp == "" {
		realIp = this.util.GetPulicIP()
	}
	if ip == "127.0.0.1" || ip == realIp {
		return true
	}
	if this.util.Contains(ip, Config().AdminIps) {
		return true
	}
	ip = "http://" + ip
	bflag = false
	for _, peer = range Config().Peers {
		if strings.HasPrefix(peer, ip) {
			bflag = true
			break
		}
	}
	return bflag
}


func (this *Server) GetClusterNotPermitMessage(r *http.Request) string {
	fmt.Println("GetClusterNotPermitMessage func is called")
	var (
		message string
	)
	message = fmt.Sprintf(CONST_MESSAGE_CLUSTER_IP, this.util.GetClientIp(r))
	return message
}

func (this *Server) GetMd5sMapByDate(date string, filename string) (*goutil.CommonMap, error) {
	fmt.Println("GetMd5sMapByDate func is called")
	var (
		err     error
		result  *goutil.CommonMap
		fpath   string
		content string
		lines   []string
		line    string
		cols    []string
		data    []byte
	)
	result = goutil.NewCommonMap(0)
	if filename == "" {
		fpath = DATA_DIR + "/" + date + "/" + CONST_FILE_Md5_FILE_NAME
	} else {
		fpath = DATA_DIR + "/" + date + "/" + filename
	}
	if !this.util.FileExists(fpath) {
		return result, errors.New(fmt.Sprintf("fpath %s not found", fpath))
	}
	if data, err = ioutil.ReadFile(fpath); err != nil {
		return result, err
	}
	content = string(data)
	lines = strings.Split(content, "\n")
	for _, line = range lines {
		cols = strings.Split(line, "|")
		if len(cols) > 2 {
			if _, err = strconv.ParseInt(cols[1], 10, 64); err != nil {
				continue
			}
			result.Add(cols[0])
		}
	}
	return result, nil
}
func (this *Server) GetMd5sByDate(date string, filename string) (mapset.Set, error) {
	fmt.Println("GetMd5sByDate func is called")
	var (
		keyPrefix string
		md5set    mapset.Set
		keys      []string
	)
	md5set = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := server.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		keys = strings.Split(string(iter.Key()), "_")
		if len(keys) >= 3 {
			md5set.Add(keys[2])
		}
	}
	iter.Release()
	return md5set, nil
}

func (this *Server) CheckScene(scene string) (bool, error) {
	fmt.Println("CheckScene func is called")
	var (
		scenes []string
	)
	if len(Config().Scenes) == 0 {
		return true, nil
	}
	for _, s := range Config().Scenes {
		scenes = append(scenes, strings.Split(s, ":")[0])
	}
	if !this.util.Contains(scene, scenes) {
		return false, errors.New("not valid scene")
	}
	return true, nil
}

func (this *Server) getRequestURI(action string) string {
	fmt.Println("getRequestURI func is called")
	var (
		uri string
	)
	if Config().SupportGroupManage {
		uri = "/" + Config().Group + "/" + action
	} else {
		uri = "/" + action
	}
	return uri
}
func (this *Server) BuildFileResult(fileInfo *FileInfo, r *http.Request) FileResult {
	fmt.Println("BuildFileResult func is called")
	var (
		outname     string
		fileResult  FileResult
		p           string
		downloadUrl string
		domain      string
		host        string
	)
	host = strings.Replace(Config().Host, "http://", "", -1)
	if r != nil {
		host = r.Host
	}
	if !strings.HasPrefix(Config().DownloadDomain, "http") {
		if Config().DownloadDomain == "" {
			Config().DownloadDomain = fmt.Sprintf("http://%s", host)
		} else {
			Config().DownloadDomain = fmt.Sprintf("http://%s", Config().DownloadDomain)
		}
	}
	if Config().DownloadDomain != "" {
		domain = Config().DownloadDomain
	} else {
		domain = fmt.Sprintf("http://%s", host)
	}
	outname = fileInfo.Name
	if fileInfo.ReName != "" {
		outname = fileInfo.ReName
	}
	p = strings.Replace(fileInfo.Path, STORE_DIR_NAME+"/", "", 1)
	if Config().SupportGroupManage {
		p = Config().Group + "/" + p + "/" + outname
	} else {
		p = p + "/" + outname
	}
	downloadUrl = fmt.Sprintf("http://%s/%s", host, p)
	if Config().DownloadDomain != "" {
		downloadUrl = fmt.Sprintf("%s/%s", Config().DownloadDomain, p)
	}
	fileResult.Url = downloadUrl
	fileResult.Md5 = fileInfo.Md5
	fileResult.Path = "/" + p
	fileResult.Domain = domain
	fileResult.Scene = fileInfo.Scene
	fileResult.Size = fileInfo.Size
	fileResult.ModTime = fileInfo.TimeStamp
	// Just for Compatibility aaa
	fileResult.Src = fileResult.Path
	fileResult.Scenes = fileInfo.Scene
	fmt.Println("aaaaaaaaaaaaa  fileResult is ",fileResult)
	return fileResult
}
func (this *Server) SaveUploadFile(file multipart.File, header *multipart.FileHeader, fileInfo *FileInfo, r *http.Request) (*FileInfo, error) {
	fmt.Println("SaveUploadFile func is called")
	var (
		err     error
		outFile *os.File
		folder  string
		fi      os.FileInfo
	)
	defer file.Close()
	_, fileInfo.Name = filepath.Split(header.Filename)
	// bugfix for ie upload file contain fullpath
	if len(Config().Extensions) > 0 && !this.util.Contains(path.Ext(fileInfo.Name), Config().Extensions) {
		return fileInfo, errors.New("(error)file extension mismatch")
	}

	if Config().RenameFile {
		fileInfo.ReName = this.util.MD5(this.util.GetUUID()) + path.Ext(fileInfo.Name)
	}
	folder = time.Now().Format("20060102")
	if fileInfo.Scene != "" {
		folder = fmt.Sprintf(STORE_DIR+"/%s/%s", fileInfo.Scene, folder)
	} else {
		//folder = fmt.Sprintf(STORE_DIR+"/%s", folder)
		folder = STORE_DIR
	}
	if fileInfo.Path != "" {
		if strings.HasPrefix(fileInfo.Path, STORE_DIR) {
			folder = fileInfo.Path
		} else {
			folder = STORE_DIR + "/" + fileInfo.Path
		}
	}
	if !this.util.FileExists(folder) {
		if err = os.MkdirAll(folder, 0775); err != nil {
			log.Error(err)
		}
	}
	outPath := fmt.Sprintf(folder+"/%s", fileInfo.Name)
	if fileInfo.ReName != "" {
		outPath = fmt.Sprintf(folder+"/%s", fileInfo.ReName)
	}
	if this.util.FileExists(outPath) && Config().EnableDistinctFile {
		for i := 0; i < 10000; i++ {
			outPath = fmt.Sprintf(folder+"/%d_%s", i, filepath.Base(header.Filename))
			fileInfo.Name = fmt.Sprintf("%d_%s", i, header.Filename)
			if !this.util.FileExists(outPath) {
				break
			}
		}
	}
	log.Info(fmt.Sprintf("upload: %s", outPath))
	if outFile, err = os.Create(outPath); err != nil {
		return fileInfo, err
	}
	defer outFile.Close()
	if err != nil {
		log.Error(err)
		return fileInfo, errors.New("(error)fail," + err.Error())
	}
	if _, err = io.Copy(outFile, file); err != nil {
		log.Error(err)
		return fileInfo, errors.New("(error)fail," + err.Error())
	}
	if fi, err = outFile.Stat(); err != nil {
		log.Error(err)
	} else {
		fileInfo.Size = fi.Size()
	}
	if fi.Size() != header.Size {
		return fileInfo, errors.New("(error)file uncomplete")
	}
	v := "" // this.util.GetFileSum(outFile, Config().FileSumArithmetic)
	if Config().EnableDistinctFile {
		v = this.util.GetFileSum(outFile, Config().FileSumArithmetic)
	} else {
		v = this.util.MD5(this.GetFilePathByInfo(fileInfo, false))
	}
	fileInfo.Md5 = v
	//fileInfo.Path = folder //strings.Replace( folder,DOCKER_DIR,"",1)
	fileInfo.Path = strings.Replace(folder, DOCKER_DIR, "", 1)
	fileInfo.Peers = append(fileInfo.Peers, this.host)
	fmt.Println("bbbbbbbbbb   upload",fileInfo)
	return fileInfo, nil
}
func (this *Server) Upload(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Upload func is called")
	var (
		err    error
		fn     string
		folder string
		fpTmp  *os.File
		fpBody *os.File
	)
/*	if r.Method == http.MethodGet {
		this.upload(w, r)
		return
	}*/
	folder = STORE_DIR + "/_tmp/" + time.Now().Format("20060102")
	if !this.util.FileExists(folder) {
		if err = os.MkdirAll(folder, 0777); err != nil {
			log.Error(err)
		}
	}
	fn = folder + "/" + this.util.GetUUID()
	defer func() {
		os.Remove(fn)
	}()
	fpTmp, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Error(err)
		w.Write([]byte(err.Error()))
		return
	}
	defer fpTmp.Close()
	if _, err = io.Copy(fpTmp, r.Body); err != nil {
		log.Error(err)
		w.Write([]byte(err.Error()))
		return
	}
	fpBody, err = os.Open(fn)
	r.Body = fpBody
	done := make(chan bool, 1)
	this.queueUpload <- WrapReqResp{&w, r, done}
	<-done
	fmt.Println("Upload End")
}

func (this *Server) SaveSmallFile(fileInfo *FileInfo) error {
	fmt.Println("SaveSmallFile func is called")
	var (
		err      error
		filename string
		fpath    string
		srcFile  *os.File
		desFile  *os.File
		largeDir string
		destPath string
		reName   string
		fileExt  string
	)
	filename = fileInfo.Name
	fileExt = path.Ext(filename)
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	fpath = DOCKER_DIR + fileInfo.Path + "/" + filename
	largeDir = LARGE_DIR + "/"
	if !this.util.FileExists(largeDir) {
		os.MkdirAll(largeDir, 0775)
	}
	reName = fmt.Sprintf("%d", this.util.RandInt(100, 300))
	destPath = largeDir + "/" + reName
	this.lockMap.LockKey(destPath)
	defer this.lockMap.UnLockKey(destPath)
	if this.util.FileExists(fpath) {
		srcFile, err = os.OpenFile(fpath, os.O_CREATE|os.O_RDONLY, 06666)
		if err != nil {
			return err
		}
		defer srcFile.Close()
		desFile, err = os.OpenFile(destPath, os.O_CREATE|os.O_RDWR, 06666)
		if err != nil {
			return err
		}
		defer desFile.Close()
		fileInfo.OffSet, err = desFile.Seek(0, 2)
		if _, err = desFile.Write([]byte("1")); err != nil {
			//first byte set 1
			return err
		}
		fileInfo.OffSet, err = desFile.Seek(0, 2)
		if err != nil {
			return err
		}
		fileInfo.OffSet = fileInfo.OffSet - 1 //minus 1 byte
		fileInfo.Size = fileInfo.Size + 1
		fileInfo.ReName = fmt.Sprintf("%s,%d,%d,%s", reName, fileInfo.OffSet, fileInfo.Size, fileExt)
		if _, err = io.Copy(desFile, srcFile); err != nil {
			return err
		}
		srcFile.Close()
		os.Remove(fpath)
		fileInfo.Path = strings.Replace(largeDir, DOCKER_DIR, "", 1)
	}
	return nil
}
func (this *Server) SendToMail(to, subject, body, mailtype string) error {
	fmt.Println("SendToMail func is called")
	host := Config().Mail.Host
	user := Config().Mail.User
	password := Config().Mail.Password
	hp := strings.Split(host, ":")
	auth := smtp.PlainAuth("", user, password, hp[0])
	var contentType string
	if mailtype == "html" {
		contentType = "Content-Type: text/" + mailtype + "; charset=UTF-8"
	} else {
		contentType = "Content-Type: text/plain" + "; charset=UTF-8"
	}
	msg := []byte("To: " + to + "\r\nFrom: " + user + ">\r\nSubject: " + "\r\n" + contentType + "\r\n\r\n" + body)
	sendTo := strings.Split(to, ";")
	err := smtp.SendMail(host, auth, user, sendTo, msg)
	return err
}
func (this *Server) BenchMark(w http.ResponseWriter, r *http.Request) {
	fmt.Println("BenchMark func is called")
	t := time.Now()
	batch := new(leveldb.Batch)
	for i := 0; i < 100000000; i++ {
		f := FileInfo{}
		f.Peers = []string{"http://192.168.0.1", "http://192.168.2.5"}
		f.Path = "20190201/19/02"
		s := strconv.Itoa(i)
		s = this.util.MD5(s)
		f.Name = s
		f.Md5 = s
		if data, err := json.Marshal(&f); err == nil {
			batch.Put([]byte(s), data)
		}
		if i%10000 == 0 {
			if batch.Len() > 0 {
				server.ldb.Write(batch, nil)
				//				batch = new(leveldb.Batch)
				batch.Reset()
			}
			fmt.Println(i, time.Since(t).Seconds())
		}
		//fmt.Println(server.GetFileInfoFromLevelDB(s))
	}
	this.util.WriteFile("time.txt", time.Since(t).String())
	fmt.Println(time.Since(t).String())
}

func (this *Server) GetStat() []StatDateFileInfo {
	fmt.Println("GetStat func is called")
	var (
		min   int64
		max   int64
		err   error
		i     int64
		rows  []StatDateFileInfo
		total StatDateFileInfo
	)
	min = 20190101
	max = 20190101
	for k := range this.statMap.Get() {
		ks := strings.Split(k, "_")
		if len(ks) == 2 {
			if i, err = strconv.ParseInt(ks[0], 10, 64); err != nil {
				continue
			}
			if i >= max {
				max = i
			}
			if i < min {
				min = i
			}
		}
	}
	for i := min; i <= max; i++ {
		s := fmt.Sprintf("%d", i)
		if v, ok := this.statMap.GetValue(s + "_" + CONST_STAT_FILE_TOTAL_SIZE_KEY); ok {
			var info StatDateFileInfo
			info.Date = s
			switch v.(type) {
			case int64:
				info.TotalSize = v.(int64)
				total.TotalSize = total.TotalSize + v.(int64)
			}
			if v, ok := this.statMap.GetValue(s + "_" + CONST_STAT_FILE_COUNT_KEY); ok {
				switch v.(type) {
				case int64:
					info.FileCount = v.(int64)
					total.FileCount = total.FileCount + v.(int64)
				}
			}
			rows = append(rows, info)
		}
	}
	total.Date = "all"
	rows = append(rows, total)
	return rows
}
func (this *Server) RegisterExit() {
	fmt.Println("RegisterExit func is called")
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for s := range c {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				this.ldb.Close()
				log.Info("Exit", s)
				os.Exit(1)
			}
		}
	}()
}
func (this *Server) AppendToQueue(fileInfo *FileInfo) {
	fmt.Println("AppendToQueue func is called")
	for (len(this.queueToPeers) + CONST_QUEUE_SIZE/10) > CONST_QUEUE_SIZE {
		time.Sleep(time.Millisecond * 50)
	}
	this.queueToPeers <- *fileInfo
}
func (this *Server) AppendToDownloadQueue(fileInfo *FileInfo) {
	fmt.Println("AppendToDownloadQueue func is called")
	for (len(this.queueFromPeers) + CONST_QUEUE_SIZE/10) > CONST_QUEUE_SIZE {
		time.Sleep(time.Millisecond * 50)
	}
	this.queueFromPeers <- *fileInfo
}
func (this *Server) ConsumerDownLoad() {
	fmt.Println("ConsumerDownLoad func is called")
	ConsumerFunc := func() {
		for {
			fileInfo := <-this.queueFromPeers
			if len(fileInfo.Peers) <= 0 {
				log.Warn("Peer is null", fileInfo)
				continue
			}
			for _, peer := range fileInfo.Peers {
				if strings.Contains(peer, "127.0.0.1") {
					log.Warn("sync error with 127.0.0.1", fileInfo)
					continue
				}
				if peer != this.host {
					this.DownloadFromPeer(peer, &fileInfo)
					break
				}
			}
		}
	}
	for i := 0; i < Config().SyncWorker; i++ {
		go ConsumerFunc()
	}
}
func (this *Server) RemoveDownloading() {
	fmt.Println("RemoveDownloading func is called")
	RemoveDownloadFunc := func() {
		for {
			iter := this.ldb.NewIterator(util.BytesPrefix([]byte("downloading_")), nil)
			for iter.Next() {
				key := iter.Key()
				keys := strings.Split(string(key), "_")
				if len(keys) == 3 {
					if t, err := strconv.ParseInt(keys[1], 10, 64); err == nil && time.Now().Unix()-t > 60*10 {
						os.Remove(DOCKER_DIR + keys[2])
					}
				}
			}
			iter.Release()
			time.Sleep(time.Minute * 3)
		}
	}
	go RemoveDownloadFunc()
}
func (this *Server) ConsumerLog() {
	fmt.Println("ConsumerLog func is called!")
	go func() {
		var (
			fileLog *FileLog
		)
		for {
			fileLog = <-this.queueFileLog
			this.saveFileMd5Log(fileLog.FileInfo, fileLog.FileName)
		}
	}()
}

func (this *Server) ConsumerPostToPeer() {
	fmt.Println("ConsumerPostToPeer func is called!")
	ConsumerFunc := func() {
		for {
			fileInfo := <-this.queueToPeers
			this.postFileToPeer(&fileInfo)
		}
	}
	for i := 0; i < Config().SyncWorker; i++ {
		go ConsumerFunc()
	}
}
func (this *Server) ConsumerUpload() {
	fmt.Println("ConsumerUpload func is called!")
	ConsumerFunc := func() {
		for {
			wr := <-this.queueUpload
			//this.upload(*wr.w, wr.r)
			this.rtMap.AddCountInt64(CONST_UPLOAD_COUNTER_KEY, wr.r.ContentLength)
			if v, ok := this.rtMap.GetValue(CONST_UPLOAD_COUNTER_KEY); ok {
				if v.(int64) > 1*1024*1024*1024 {
					var _v int64
					this.rtMap.Put(CONST_UPLOAD_COUNTER_KEY, _v)
					debug.FreeOSMemory()
				}
			}
			wr.done <- true
		}
	}
	for i := 0; i < Config().UploadWorker; i++ {
		go ConsumerFunc()
	}
}

func (this *Server) CleanLogLevelDBByDate(date string, filename string) {
	fmt.Println("CleanLogLevelDBByDate func is called!")
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("CleanLogLevelDBByDate")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	var (
		err       error
		keyPrefix string
		keys      mapset.Set
	)
	keys = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := server.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		keys.Add(string(iter.Value()))
	}
	iter.Release()
	for key := range keys.Iter() {
		err = this.RemoveKeyFromLevelDB(key.(string), this.logDB)
		if err != nil {
			log.Error(err)
		}
	}
}

func (this *Server) LoadFileInfoByDate(date string, filename string) (mapset.Set, error) {
	fmt.Println("LoadFileInfoByDate func is called!")
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("LoadFileInfoByDate")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()
	var (
		err       error
		keyPrefix string
		fileInfos mapset.Set
	)
	fileInfos = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := server.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		var fileInfo FileInfo
		if err = json.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}
		fileInfos.Add(&fileInfo)
	}
	iter.Release()
	return fileInfos, nil
}

func (this *Server) LoadQueueSendToPeer() {
	fmt.Println("LoadQueueSendToPeer func is called!")
	if queue, err := this.LoadFileInfoByDate(this.util.GetToDay(), CONST_Md5_QUEUE_FILE_NAME); err != nil {
		log.Error(err)
	} else {
		for fileInfo := range queue.Iter() {
			//this.queueFromPeers <- *fileInfo.(*FileInfo)
			this.AppendToDownloadQueue(fileInfo.(*FileInfo))
		}
	}
}

func (this *Server) CheckClusterStatus() {
	fmt.Println("CheckClusterStatus is called!!")
	check := func() {
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				log.Error("CheckClusterStatus")
				log.Error(re)
				log.Error(string(buffer))
			}
		}()
		var (
			status  JsonResult
			err     error
			subject string
			body    string
			req     *httplib.BeegoHTTPRequest
		)
		for _, peer := range Config().Peers {
			req = httplib.Get(fmt.Sprintf("%s%s", peer, this.getRequestURI("status")))
			req.SetTimeout(time.Second*5, time.Second*5)
			err = req.ToJSON(&status)
			if err != nil || status.Status != "ok" {
				for _, to := range Config().AlarmReceivers {
					subject = "fastdfs server error"
					if err != nil {
						body = fmt.Sprintf("%s\nserver:%s\nerror:\n%s", subject, peer, err.Error())
					} else {
						body = fmt.Sprintf("%s\nserver:%s\n", subject, peer)
					}
					if err = this.SendToMail(to, subject, body, "text"); err != nil {
						log.Error(err)
					}
				}
				if Config().AlarmUrl != "" {
					req = httplib.Post(Config().AlarmUrl)
					req.SetTimeout(time.Second*10, time.Second*10)
					req.Param("message", body)
					req.Param("subject", subject)
					if _, err = req.String(); err != nil {
						log.Error(err)
					}
				}
			}
		}
	}
	go func() {
		for {
			time.Sleep(time.Minute * 10)
			check()
		}
	}()
}

func (this *Server) Index(w http.ResponseWriter, r *http.Request) {
	var (
		uploadUrl    string
		uploadBigUrl string
		uppy         string
	)
	uploadUrl = "/upload"
	uploadBigUrl = CONST_BIG_UPLOAD_PATH_SUFFIX
	if Config().EnableWebUpload {
		if Config().SupportGroupManage {
			uploadUrl = fmt.Sprintf("/%s/upload", Config().Group)
			uploadBigUrl = fmt.Sprintf("/%s%s", Config().Group, CONST_BIG_UPLOAD_PATH_SUFFIX)
		}
		fmt.Println("Index func is beginning ")
		fmt.Println("uploadBigUrl is", uploadBigUrl)
		uppy = `<html>
			  
			  <head>
				<meta charset="utf-8" />
				<title>go-fastdfs</title>
				<style>form { bargin } .form-line { display:block;height: 30px;margin:8px; } #stdUpload {background: #fafafa;border-radius: 10px;width: 745px; }</style>
				<link href="https://transloadit.edgly.net/releases/uppy/v0.30.0/dist/uppy.min.css" rel="stylesheet"></head>
			  
			  <body>
                <div>标准上传(强列建议使用这种方式)</div>
				<div id="stdUpload">	
				  <form action="%s" method="post" enctype="multipart/form-data">	
					<span class="form-line">场景(scene):
					  <input type="text" id="scene" name="scene" value="%s" /></span>
					<input type="submit" name="submit" value="upload" />
                </form>
				</div>
                 <div>断点续传（如果文件很大时可以考虑）</div>
				<div>
				 
				  <div id="drag-drop-area"></div>
				  <script src="https://transloadit.edgly.net/releases/uppy/v0.30.0/dist/uppy.min.js"></script>
				  <script>var uppy = Uppy.Core().use(Uppy.Dashboard, {
					  inline: true,
					  target: '#drag-drop-area'
					}).use(Uppy.Tus, {
					  endpoint: '%s'
					})
					uppy.on('complete', (result) => {
					 // console.log(result) console.log('Upload complete! We’ve uploaded these files:', result.successful)
					})
					//uppy.setMeta({ auth_token: '9ee60e59-cb0f-4578-aaba-29b9fc2919ca',callback_url:'http://127.0.0.1/callback' ,filename:'自定义文件名','path':'自定义path',scene:'自定义场景' })//这里是传递上传的认证参数,callback_url参数中 id为文件的ID,info 文转的基本信息json
					uppy.setMeta({ auth_token: '9ee60e59-cb0f-4578-aaba-29b9fc2919ca',callback_url:'http://127.0.0.1/callback'})//自定义参数与普通上传类似（虽然支持自定义，建议不要自定义，海量文件情况下，自定义很可能给自已给埋坑）
                </script>
				</div>
			  </body>
			</html>`
		uppyFileName := STATIC_DIR + "/uppy.html"
		if this.util.IsExist(uppyFileName) {
			if data, err := this.util.ReadBinFile(uppyFileName); err != nil {
				log.Error(err)
			} else {
				uppy = string(data)
			}
		} else {
			this.util.WriteFile(uppyFileName, uppy)
		}
		fmt.Fprintf(w,
			fmt.Sprintf(uppy, uploadUrl, Config().DefaultScene, uploadBigUrl))
	} else {
		w.Write([]byte("web upload deny"))
	}
	fmt.Println("Index func ends! ")
}
func init() {
	flag.Parse()
	if *v {
		fmt.Printf("%s\n%s\n%s\n%s\n", VERSION, BUILD_TIME, GO_VERSION, GIT_VERSION)
		os.Exit(0)
	}
	appDir, e1 := filepath.Abs(filepath.Dir(os.Args[0]))
	curDir, e2 := filepath.Abs(".")
	if e1 == nil && e2 == nil && appDir != curDir {
		msg := fmt.Sprintf("please change directory to '%s' start fileserver\n", appDir)
		msg = msg + fmt.Sprintf("请切换到 '%s' 目录启动 fileserver ", appDir)
		log.Warn(msg)
		fmt.Println(msg)
		os.Exit(1)
	}
	DOCKER_DIR = os.Getenv("GO_FASTDFS_DIR")
	if DOCKER_DIR != "" {
		if !strings.HasSuffix(DOCKER_DIR, "/") {
			DOCKER_DIR = DOCKER_DIR + "/"
		}
	}
	STORE_DIR = DOCKER_DIR + STORE_DIR_NAME
	CONF_DIR = DOCKER_DIR + CONF_DIR_NAME
	DATA_DIR = DOCKER_DIR + DATA_DIR_NAME
	LOG_DIR = DOCKER_DIR + LOG_DIR_NAME
	STATIC_DIR = DOCKER_DIR + STATIC_DIR_NAME
	LARGE_DIR_NAME = "haystack"
	LARGE_DIR = STORE_DIR + "/haystack"
	CONST_LEVELDB_FILE_NAME = DATA_DIR + "/fileserver.db"
	CONST_LOG_LEVELDB_FILE_NAME = DATA_DIR + "/log.db"
	CONST_STAT_FILE_NAME = DATA_DIR + "/stat.json"
	CONST_CONF_FILE_NAME = CONF_DIR + "/cfg.json"
	FOLDERS = []string{DATA_DIR, STORE_DIR, CONF_DIR, STATIC_DIR}
	logAccessConfigStr = strings.Replace(logAccessConfigStr, "{DOCKER_DIR}", DOCKER_DIR, -1)
	logConfigStr = strings.Replace(logConfigStr, "{DOCKER_DIR}", DOCKER_DIR, -1)
	for _, folder := range FOLDERS {
		os.MkdirAll(folder, 0775)
	}
	server = NewServer()

	if !server.util.FileExists(CONST_CONF_FILE_NAME) {
		var ip string
		if ip = os.Getenv("GO_FASTDFS_IP"); ip == "" {
			ip = server.util.GetPulicIP()
		}
		server.util.WriteFile(CONST_CONF_FILE_NAME, cfgJson)
	}
	if logger, err := log.LoggerFromConfigAsBytes([]byte(logConfigStr)); err != nil {
		panic(err)
	} else {
		log.ReplaceLogger(logger)
	}
	if _logacc, err := log.LoggerFromConfigAsBytes([]byte(logAccessConfigStr)); err == nil {
		logacc = _logacc
		log.Info("succes init log access")
	} else {
		log.Error(err.Error())
	}
	ParseConfig(CONST_CONF_FILE_NAME)
	if Config().QueueSize == 0 {
		Config().QueueSize = CONST_QUEUE_SIZE
	}
	if Config().SupportGroupManage {
		staticHandler = http.StripPrefix("/"+Config().Group+"/", http.FileServer(http.Dir(STORE_DIR)))
	} else {
		staticHandler = http.StripPrefix("/", http.FileServer(http.Dir(STORE_DIR)))
	}
	server.initComponent(false)
}
type hookDataStore struct {
	tusd.DataStore
}
type httpError struct {
	error
	statusCode int
}

func (err httpError) StatusCode() int {
	fmt.Println("StatusCode func is called!!")
	fmt.Println("StatusCode is ", err.statusCode)
	return err.statusCode
}
func (err httpError) Body() []byte {
	return []byte(err.Error())
}
//cancal autConfig
func (store hookDataStore) NewUpload(info tusd.FileInfo) (id string, err error) {
	fmt.Println("NewUpload is called!!")
	fmt.Println("FileInfo is ",info)
	return store.DataStore.NewUpload(info)
}

func (this *Server) initTus() {
	var (
		err     error
		fileLog *os.File
		bigDir  string
	)
	fmt.Println("tus is beginning")
	BIG_DIR := STORE_DIR + "/_big/"
	os.MkdirAll(BIG_DIR, 0775)
	os.MkdirAll(LOG_DIR, 0775)
	store := filestore.FileStore{
		Path: BIG_DIR,
	}
	if fileLog, err = os.OpenFile(LOG_DIR+"/tusd.log", os.O_CREATE|os.O_RDWR, 0666); err != nil {
		log.Error(err)
		panic("initTus")
	}
	go func() {
		for {
			if fi, err := fileLog.Stat(); err != nil {
				log.Error(err)
			} else {
				if fi.Size() > 1024*1024*500 {
					//500M
					this.util.CopyFile(LOG_DIR+"/tusd.log", LOG_DIR+"/tusd.log.2")
					fileLog.Seek(0, 0)
					fileLog.Truncate(0)
					fileLog.Seek(0, 2)
				}
			}
			time.Sleep(time.Second * 30)
		}
	}()
	l := slog.New(fileLog, "[tusd] ", slog.LstdFlags)
	bigDir = CONST_BIG_UPLOAD_PATH_SUFFIX
	if Config().SupportGroupManage {
		bigDir = fmt.Sprintf("/%s%s", Config().Group, CONST_BIG_UPLOAD_PATH_SUFFIX)
	}
	composer := tusd.NewStoreComposer()
	// support raw tus upload and download
	store.GetReaderExt = func(id string) (io.Reader, error) {
		var (
			offset int64
			err    error
			length int
			buffer []byte
			fi     *FileInfo
			fn     string
		)
		if fi, err = this.GetFileInfoFromLevelDB(id); err != nil {
			log.Error(err)
			return nil, err
		} else {
			fn = fi.Name
			if fi.ReName != "" {
				fn = fi.ReName
			}
			fp := DOCKER_DIR + fi.Path + "/" + fn
			if this.util.FileExists(fp) {
				log.Info(fmt.Sprintf("download:%s", fp))
				return os.Open(fp)
			}
			ps := strings.Split(fp, ",")
			if len(ps) > 2 && this.util.FileExists(ps[0]) {
				if length, err = strconv.Atoi(ps[2]); err != nil {
					return nil, err
				}
				if offset, err = strconv.ParseInt(ps[1], 10, 64); err != nil {
					return nil, err
				}
				if buffer, err = this.util.ReadFileByOffSet(ps[0], offset, length); err != nil {
					return nil, err
				}
				if buffer[0] == '1' {
					bufferReader := bytes.NewBuffer(buffer[1:])
					return bufferReader, nil
				} else {
					msg := "data no sync"
					log.Error(msg)
					return nil, errors.New(msg)
				}
			}
			return nil, errors.New(fmt.Sprintf("%s not found", fp))
		}
	}
	store.UseIn(composer)
	SetupPreHooks := func(composer *tusd.StoreComposer) {
		composer.UseCore(hookDataStore{
			DataStore: composer.Core,
		})
	}
	SetupPreHooks(composer)
	handler, err := tusd.NewHandler(tusd.Config{
		Logger:                  l,
		BasePath:                bigDir,
		StoreComposer:           composer,
		NotifyCompleteUploads:   true,
		RespectForwardedHeaders: true,
	})
	notify := func(handler *tusd.Handler) {
		for {
			select {
			case info := <-handler.CompleteUploads:
				log.Info("CompleteUploads", info)
				name := ""
				pathCustom := ""
				scene := Config().DefaultScene
				if v, ok := info.MetaData["filename"]; ok {
					name = v
				}
				if v, ok := info.MetaData["scene"]; ok {
					scene = v
				}
				if v, ok := info.MetaData["path"]; ok {
					pathCustom = v
				}
				var err error
				md5sum := ""
				oldFullPath := BIG_DIR + "/" + info.ID + ".bin"
				infoFullPath := BIG_DIR + "/" + info.ID + ".info"
				if md5sum, err = this.util.GetFileSumByName(oldFullPath, Config().FileSumArithmetic); err != nil {
					log.Error(err)
					continue
				}
				ext := path.Ext(name)
				filename := md5sum + ext
				if name != "" {
					filename = name
				}
				if Config().RenameFile {
					filename = md5sum + ext
				}
				timeStamp := time.Now().Unix()
				fpath := time.Now().Format("/20060102")
				if pathCustom != "" {
					fpath = "/" + strings.Replace(pathCustom, ".", "", -1) + "/"
				}
				newFullPath := STORE_DIR + "/" + scene + fpath + "/" + filename
				if pathCustom != "" {
					newFullPath = STORE_DIR + "/" + scene + fpath + filename
				}
				if fi, err := this.GetFileInfoFromLevelDB(md5sum); err != nil {
					log.Error(err)
				} else {
					tpath := this.GetFilePathByInfo(fi, true)
					if fi.Md5 != "" && this.util.FileExists(tpath) {
						if _, err := this.SaveFileInfoToLevelDB(info.ID, fi, this.ldb); err != nil {
							log.Error(err)
						}
						log.Info(fmt.Sprintf("file is found md5:%s", fi.Md5))
						log.Info("remove file:", oldFullPath)
						log.Info("remove file:", infoFullPath)
						os.Remove(oldFullPath)
						os.Remove(infoFullPath)
						continue
					}
				}
				fpath2 := ""
				fpath2 = STORE_DIR_NAME + "/" + Config().DefaultScene + fpath
				if pathCustom != "" {
					fpath2 = STORE_DIR_NAME + "/" + Config().DefaultScene + fpath
					fpath2 = strings.TrimRight(fpath2, "/")
				}
				os.MkdirAll(DOCKER_DIR+fpath2, 0775)
				fileInfo := &FileInfo{
					Name:      name,
					Path:      fpath2,
					ReName:    filename,
					Size:      info.Size,
					TimeStamp: timeStamp,
					Md5:       md5sum,
					Peers:     []string{this.host},
					OffSet:    -1,
				}
				if err = os.Rename(oldFullPath, newFullPath); err != nil {
					log.Error(err)
					continue
				}
				log.Info(fileInfo)
				os.Remove(infoFullPath)
				if _, err = this.SaveFileInfoToLevelDB(info.ID, fileInfo, this.ldb); err != nil {
					//assosiate file id
					log.Error(err)
				}
				this.SaveFileMd5Log(fileInfo, CONST_FILE_Md5_FILE_NAME)
				go this.postFileToPeer(fileInfo)
				callBack := func(info tusd.FileInfo, fileInfo *FileInfo) {
					if callback_url, ok := info.MetaData["callback_url"]; ok {
						req := httplib.Post(callback_url)
						req.SetTimeout(time.Second*10, time.Second*10)
						req.Param("info", server.util.JsonEncodePretty(fileInfo))
						req.Param("id", info.ID)
						if _, err := req.String(); err != nil {
							log.Error(err)
						}
					}
				}
				go callBack(info, fileInfo)
			}
		}
	}
	go notify(handler)
	if err != nil {
		log.Error(err)
	}
	fmt.Println("UpFile info is",bigDir)
	fmt.Println("InitTus  ends!! ")
	http.Handle(bigDir, http.StripPrefix(bigDir, handler))
}

func (this *Server) FormatStatInfo() {
	fmt.Println("FormatStatInfo func is called!!")
	var (
		data  []byte
		err   error
		count int64
		stat  map[string]interface{}
	)
	if this.util.FileExists(CONST_STAT_FILE_NAME) {
		if data, err = this.util.ReadBinFile(CONST_STAT_FILE_NAME); err != nil {
			log.Error(err)
		} else {
			if err = json.Unmarshal(data, &stat); err != nil {
				log.Error(err)
			} else {
				for k, v := range stat {
					switch v.(type) {
					case float64:
						vv := strings.Split(fmt.Sprintf("%f", v), ".")[0]
						if count, err = strconv.ParseInt(vv, 10, 64); err != nil {
							log.Error(err)
						} else {
							this.statMap.Put(k, count)
						}
					default:
						this.statMap.Put(k, v)
					}
				}
			}
		}
	}
}
func (this *Server) initComponent(isReload bool) {
	var (
		ip string
	)
	if ip = os.Getenv("GO_FASTDFS_IP"); ip == "" {
		ip = this.util.GetPulicIP()
	}
	if Config().Host == "" {
		if len(strings.Split(Config().Addr, ":")) == 2 {
			server.host = fmt.Sprintf("http://%s:%s", ip, strings.Split(Config().Addr, ":")[1])
			Config().Host = server.host
		}
	} else {
		if strings.HasPrefix(Config().Host, "http") {
			server.host = Config().Host
		} else {
			server.host = "http://" + Config().Host
		}
	}
	ex, _ := regexp.Compile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	var peers []string
	for _, peer := range Config().Peers {
		if this.util.Contains(ip, ex.FindAllString(peer, -1)) ||
			this.util.Contains("127.0.0.1", ex.FindAllString(peer, -1)) {
			continue
		}
		if strings.HasPrefix(peer, "http") {
			peers = append(peers, peer)
		} else {
			peers = append(peers, "http://"+peer)
		}
	}
	Config().Peers = peers
	if !isReload {
		this.FormatStatInfo()
		if Config().EnableTus {
			this.initTus()
		}
	}
	for _, s := range Config().Scenes {
		kv := strings.Split(s, ":")
		if len(kv) == 2 {
			this.sceneMap.Put(kv[0], kv[1])
		}
	}
	if Config().ReadTimeout == 0 {
		Config().ReadTimeout = 60 * 10
	}
	if Config().WriteTimeout == 0 {
		Config().WriteTimeout = 60 * 10
	}
	if Config().SyncWorker == 0 {
		Config().SyncWorker = 200
	}
	if Config().UploadWorker == 0 {
		Config().UploadWorker = runtime.NumCPU() + 4
		if runtime.NumCPU() < 4 {
			Config().UploadWorker = 8
		}
	}
	if Config().UploadQueueSize == 0 {
		Config().UploadQueueSize = 200
	}
	if Config().RetryCount == 0 {
		Config().RetryCount = 3
	}
	if Config().SyncDelay == 0 {
		Config().SyncDelay = 60
	}
	if Config().WatchChanSize == 0 {
		Config().WatchChanSize = 100000
	}
}

type HttpHandler struct {
}

func (HttpHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	fmt.Println("ServeHTTP func is called!!")
	status_code := "200"
	defer func(t time.Time) {
		logStr := fmt.Sprintf("[Access] %s | %s | %s | %s | %s |%s",
			time.Now().Format("2006/01/02 - 15:04:05"),
			//res.Header(),
			time.Since(t).String(),
			server.util.GetClientIp(req),
			req.Method,
			status_code,
			req.RequestURI,
		)
		logacc.Info(logStr)
	}(time.Now())
	defer func() {
		if err := recover(); err != nil {
			status_code = "500"
			res.WriteHeader(500)
			print(err)
			buff := debug.Stack()
			log.Error(err)
			log.Error(string(buff))
		}
	}()
/*	if Config().EnableCrossOrigin {
		server.CrossOrigin(res, req)
	}*/
	fmt.Println("ServeHTTP  status_code",status_code)
	http.DefaultServeMux.ServeHTTP(res, req)
}
func (this *Server) Main() {
	go func() {
		for {
			this.CheckFileAndSendToPeer(this.util.GetToDay(), CONST_Md5_ERROR_FILE_NAME, false)
			//fmt.Println("CheckFileAndSendToPeer")
			time.Sleep(time.Second * time.Duration(Config().RefreshInterval))
			//this.util.RemoveEmptyDir(STORE_DIR)
		}
	}()

	go this.CheckClusterStatus()
	go this.LoadQueueSendToPeer()
	go this.ConsumerPostToPeer()
	go this.ConsumerLog()
	go this.ConsumerDownLoad()
	go this.ConsumerUpload()
	go this.RemoveDownloading()
	if Config().EnableFsnotify {
		go this.WatchFilesChange()
	}

	go func() { // force free memory
		for {
			time.Sleep(time.Minute * 1)
			debug.FreeOSMemory()
		}
	}()
	uploadPage := "upload.html"
	http.HandleFunc(fmt.Sprintf("%s", "/"), this.Download)
	http.HandleFunc(fmt.Sprintf("/%s", uploadPage), this.Index)
	fmt.Println("Listen on " + Config().Addr)
	srv := &http.Server{
		Addr:              Config().Addr,
		Handler:           new(HttpHandler),
		ReadTimeout:       time.Duration(Config().ReadTimeout) * time.Second,
		ReadHeaderTimeout: time.Duration(Config().ReadHeaderTimeout) * time.Second,
		WriteTimeout:      time.Duration(Config().WriteTimeout) * time.Second,
		IdleTimeout:       time.Duration(Config().IdleTimeout) * time.Second,
	}
	err := srv.ListenAndServe()
	log.Error(err)
}
func saveImage(image *upload.Image, fileName string) error {
	absFilePath := utils.AbsPath()
	fileName = fmt.Sprintf("%s/%s", absFilePath, fileName)
	//fileName := "/root/go/docker/busybox.tar"
	//fmt.Sprintf("%s:%s", image.OldName, image.OldTag)
	registry := &upload.ImageRegistry{
		Username: "bjyimaike@163.com",
		Password: "emcc7556",
		ServerAddress: "registry.cn-beijing.aliyuncs.com",
	}
	var client *docker.Client = upload.NewClient()
	//var image *upload.Image
	image = &upload.Image {
		Name: "registry.cn-beijing.aliyuncs.com/hiacloud/busybox",
		Tag: "latest",
		OldName: "busybox",
		OldTag: "latest",
		Registry: registry,
	}
	mes, err := image.LoadImage(client, fileName)
	if err != nil {
		fmt.Println("Load Image is fail")
		return err
	}else {
		fmt.Println(mes.Mes)
		mes, err = image.TagImage(client, image)
		if err != nil {
			fmt.Println("Tag Image is fail")
			return err
		}else {
			fmt.Println(mes.Mes)
			mes, err = image.PushImageCustomRegistry(client, image)
			if err != nil {
				fmt.Println("Push Image is fail")
				return  err
			}else {
				fmt.Println(mes.Mes)
				return nil
			}
		}

	}
}

func main() {
	server.Main()
	fmt.Println("server info ", server)
	// save upload

}
