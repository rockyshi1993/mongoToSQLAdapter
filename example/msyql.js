const mysql = require('mysql2/promise');

// 创建一个连接池（可以根据需要调整配置）
const pool = mysql.createPool({
    host: 'localhost',          // MySQL 服务器地址
    user: 'root',               // MySQL 用户名
    password: '',               // MySQL 密码
    database: 'my_databases',   // 连接的数据库
    port: 3307,                 // 端口（如果改了端口，调整这里）
    waitForConnections: true,   // 是否等待可用连接
    connectionLimit: 10,        // 连接池最大连接数
    queueLimit: 0               // 队列请求数量限制（0 表示不限制）
});

// 利用 async/await 将流转换为数组
async function streamToArray(stream) {
    const rows = [];
    // 利用 for-await-of 遍历流中的每一项
    for await (const row of stream) {
        rows.push(row);
    }
    return rows;
}

/**
 * sql 查询
 * @param sql 带占位符 sql
 * @param params 参数
 * @param isStream 是否以 stream 流返回
 * @returns {Promise<*|*[]>}
 */
async function query(sql, params = [], isStream ) {
    let connection;
    try {
        // 获取 Promise 封装的连接
        connection = await pool.getConnection();

        if(isStream){
            // 该对象支持回调风格的 query() 方法，并可生成流
            const rawConnection = connection.connection;
            const q = rawConnection.query(sql, params);

            // 将 Query 对象转换为 Node.js 可读流
            const stream = q.stream();

            // 当流结束或发生错误时，释放连接
            stream.on('end', () => {
                connection.release();
            });
            stream.on('error', (err) => {
                // 此处可增加错误日志记录
                connection.release();
            });

            return await streamToArray(stream);
        }else{
            const [rows] = await connection.execute(sql, params);
            return rows;
        }

    } catch (error) {
        if (connection) connection.release();
        throw error;
    }
}

/**
 * 关闭连接池
 */
async function closePool() {
    try {
        await pool.end();
        console.log('✅ MySQL 连接池已关闭');
    } catch (error) {
        console.error('❌ 关闭 MySQL 连接池失败:', error);
    }
}

// 监听应用退出事件，确保连接池被正常关闭
process.on('SIGINT', async () => {
    console.log('🛑 进程终止，关闭 MySQL 连接池...');
    await closePool();
    process.exit(0);
});

module.exports = { query, closePool, pool };
