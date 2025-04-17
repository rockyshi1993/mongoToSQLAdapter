// 定义默认的 Logger 对象，包含 debug、info、error 三个方法

const logger = (logger)=>{
    return logger || {
        debug: (msg, ...args) => console.debug(msg, ...args),
        info: (msg, ...args) => console.log(msg, ...args),
        error: (msg, ...args) => console.error(msg, ...args)
    };
}
module.exports = logger;