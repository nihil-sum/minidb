# 使用官方 Go 语言基础镜像 (选择较为稳定的 1.22 或 1.23 版本)
FROM golang:1.23-bookworm

# 设置容器内的工作目录
WORKDIR /app

# 优化构建缓存：先只复制依赖文件
# 这样如果你的代码变了但依赖没变，Docker 不需要重新下载包
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制项目的剩余所有源代码到容器中
COPY . .

# (可选) 尝试预编译代码，确保没有编译错误
# 如果你的项目有 main.go 入口，使用：RUN go build -o main .
# 如果只是测试库，使用：RUN go build ./...
RUN go build -o dbms_engine .

# 设置容器启动时的默认命令
# 这里设置为运行所有测试，并显示详细输出
# 老师一运行容器就能看到测试结果，非常直观
CMD ["go", "test", "-v", "./..."]