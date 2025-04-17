"use strict";

/* ============================================================
   配置与全局日志、错误、插件定义
============================================================ */

const LOG_LEVELS = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };

const Config = {
    LOG_LEVEL: Object.keys(LOG_LEVELS).includes((process.env.LOG_LEVEL && process.env.LOG_LEVEL.toUpperCase()) || '')
        ? process.env.LOG_LEVEL.toUpperCase()
        : "ERROR"
};

// 如果配置中的日志级别无效，默认使用 ERROR
let currentLogLevel = LOG_LEVELS[Config.LOG_LEVEL] || LOG_LEVELS.ERROR;

const Logger = {
    enabled: false, // 允许关闭日志
    setLevel(levelStr) {
        currentLogLevel = LOG_LEVELS[levelStr] || LOG_LEVELS.ERROR;
    },
    debug: (...args) => {
        if (Logger.enabled && currentLogLevel <= LOG_LEVELS.DEBUG) console.debug("[DEBUG]", ...args);
    },
    info: (...args) => {
        if (Logger.enabled && currentLogLevel <= LOG_LEVELS.INFO) console.info("[INFO]", ...args);
    },
    warn: (...args) => {
        if (Logger.enabled && currentLogLevel <= LOG_LEVELS.WARN) console.warn("[WARN]", ...args);
    },
    error: (...args) => {
        if (Logger.enabled && currentLogLevel <= LOG_LEVELS.ERROR) console.error("[ERROR]", ...args);
    }
};

/**
 * 简单日志封装函数
 */
function log(level, message, ...args) {
    const validLevel = level && level.toUpperCase();
    if (LOG_LEVELS[validLevel] !== undefined && currentLogLevel <= LOG_LEVELS[validLevel]) {
        const logMethod = console[validLevel.toLowerCase()] || console.log;
        logMethod(`[${validLevel}]`, message, ...args);
    }
}


/**
 * 错误类型定义
 */
class QueryParseError extends Error {
    constructor(message, context) {
        super(`[QueryParseError] ${message} | Context: ${context}`);
        this.name = "QueryParseError";
    }
}

class SQLGenerationError extends Error {
    constructor(message, context) {
        super(`[SQLGenerationError] ${message} | Context: ${context}`);
        this.name = "SQLGenerationError";
    }
}

/**
 * 统一错误处理函数
 */
function handleError(errMsg, context, ErrorType) {
    Logger.error(`[${ErrorType.name}] ${errMsg} | Context:`, context,'-----------');
    throw new ErrorType(`${errMsg} | Context: ${JSON.stringify(context)}`);
}

/**
 * 操作符映射：MongoDB 与 MySQL 之间的对照
 */
const OPERATORS_MYSQL = {
    $eq: '=',
    $ne: '<>',
    $gt: '>',
    $gte: '>=',
    $lt: '<',
    $lte: '<=',
    $in: 'IN',
    $nin: 'NOT IN',
    $and: 'AND',
    $or: 'OR',
    $not: 'NOT',
    $regex: 'REGEXP',
    $like: 'LIKE'
};

/**
 * 插件扩展机制：支持自定义操作符转换
 */
const OperatorPlugins = []; // 改为数组存放插件对象

class OperatorPlugin {
    constructor(operator, handler) {
        this.operator = operator;
        this.handler = handler;
    }
    apply(field, opValue, conditions, params, context) {
        this.handler(field, opValue, conditions, params, context);
    }
}

function registerOperatorPlugin(operator, handler) {
    OperatorPlugins.push(new OperatorPlugin(operator, handler));
}

/* ============================================================
   公共 SQL 生成函数
============================================================ */
/**
 * 根据传入配置拼接 SQL 语句
 * @param {Object} queryConfig
 * @returns {{sql: string, params: Array}}
 */
function generateSQL(queryConfig) {
    // 增加默认值，防止未传字段时报错
    let {
        selectClause = "*",
        whereClause = "",
        joinClause = "",
        sortClause = "",
        limitClause = "",
        offsetClause = "",
        params = [],
        tableName
    } = queryConfig;
    let sql = `SELECT ${selectClause} FROM ${tableName}`;
    if (joinClause) sql += ` ${joinClause}`;
    if (whereClause) sql += ` WHERE ${whereClause}`;
    if (sortClause) sql += ` ORDER BY ${sortClause}`;
    if (limitClause) sql += ` LIMIT ${limitClause}`;
    if (offsetClause) sql += ` OFFSET ${offsetClause}`;
    return { sql, params };
}

/* ============================================================
   MongoDB 查询转换为 MySQL 查询函数
============================================================ */
function parseMongoQuery(query, params, context = "root") {
    let conditions = [];

    for (let key in query) {
        
        // ===== 修改开始: 添加 $expr 支持 =====
        if (key === "$expr") {
            // 使用 convertExpr 将 $expr 内的聚合表达式转换为 SQL 表达式
            const exprSQL = convertExpr(query[key]);
            conditions.push(exprSQL);
            continue;
        }
        // ===== 修改结束: 添加 $expr 支持 =====

        const value = query[key];
        try {
            if (key.startsWith('$')) {
                Logger.debug(`处理逻辑操作符 (${context}):`, key, value);
                if (key === '$or') {
                    let orConditions = value.map((subQuery, idx) =>
                        parseMongoQuery(subQuery, params, `${context}->OR[${idx}]`)
                    ).filter(cond => cond && cond !== "1=1");

                    if (orConditions.length > 0) {
                        conditions.push(`(${orConditions.join(" OR ")})`);
                    } else {
                        conditions.push("1=0"); // 修复：空 $or 应返回 `1=0`
                    }
                }
                else if (key === '$nor') {
                    let norConditions = value.map((subQuery, idx) =>
                        parseMongoQuery(subQuery, params, `${context}->NOR[${idx}]`)
                    ).filter(cond => cond && cond !== "1=1");
                    if (norConditions.length) {
                        conditions.push(`NOT (${norConditions.join(" OR ")})`);
                    } else {
                        conditions.push("1=1");
                    }
                } else if (key === '$and') {
                    let andConditions = value.map((subQuery, idx) =>
                        parseMongoQuery(subQuery, params, `${context}->AND[${idx}]`)
                    ).filter(cond => cond);
                    if (andConditions.length) {
                        conditions.push(andConditions.join(" AND "));
                    }
                } else {
                    handleLogicalOperators(key, value, conditions, params, context);
                }
            } else {
                // 普通字段条件处理
                if (value instanceof SubQuery) {
                    Logger.debug(`子查询检测 (${context})，字段:`, key);
                    const subResult = value.toSQL();
                    conditions.push(`${key} IN ${subResult}`);
                    params.push(...value.getParams());
                } else if (typeof value === 'object' && !Array.isArray(value) && value !== null) {
                    let fieldConds = [];
                    for (let op in value) {
                        if (op.startsWith('$')) {
                            if (op === "$in" && value[op] instanceof SubQuery) {
                                Logger.debug(`子查询 in 操作符 (${context})，字段:`, key);
                                const subResult = value[op].toSQL();
                                fieldConds.push(`${key} IN ${subResult}`);
                                params.push(...value[op].getParams());
                            } else if (['$in', '$nin', '$all'].includes(op)) {
                                handleArrayOperators(key, value[op], op, fieldConds, params, context);
                            } else {
                                handleOperator(key, value[op], op, fieldConds, params, context);
                            }
                        } else {
                            fieldConds.push(`${key} = ?`);
                            params.push(value[op]);
                        }
                    }
                    if (fieldConds.length > 1) {
                        conditions.push(`(${fieldConds.join(" AND ")})`);
                    } else if (fieldConds.length === 1) {
                        conditions.push(fieldConds[0]);
                    }
                } else {
                    conditions.push(`${key} = ?`);
                    params.push(value);
                }
            }
        } catch (e) {
            const errMsg = `Error processing key "${key}" in context "${context}": ${e.message}`;
            Logger.error(errMsg);
            throw new QueryParseError(e.message, context);
        }
    }

    const result = conditions.length ? conditions.join(" AND ") : "1=1";
    Logger.debug(`生成的 WHERE 子句 (${context}):`, result, "参数:", params);
    return result;
}

function handleLogicalOperators(operator, value, conditions, params, context) {
    if (!Array.isArray(value) || value.length === 0) {
        conditions.push(operator === '$and' ? "1=1" : "1=0");
        return;
    }
    let subConds = value.map((subQuery, idx) =>
        "(" + parseMongoQuery(subQuery, params, `${context}->${operator}[${idx}]`) + ")"
    );
    if (operator === '$nor') {
        conditions.push("NOT (" + subConds.join(" OR ") + ")");
    } else {
        conditions.push("(" + subConds.join(" " + (OPERATORS_MYSQL[operator] || operator) + " ") + ")");
    }
    Logger.debug(`逻辑操作符处理 (${context}) ${operator}:`, conditions[conditions.length - 1]);
}

function handleArrayOperators(field, values, operator, conditions, params, context) {
    if (!Array.isArray(values) || values.length === 0) {
        Logger.warn(`数组操作符 (${context}) ${operator} 的值为空，跳过字段 ${field}`);
        // 修复 `$in: []` 变为 `1=0`
        if (operator === '$in') {
            conditions.push("1=0");
        }
        // 修复 `$nin: []` 变为 `1=1`
        else if (operator === '$nin') {
            conditions.push("1=1");
        }
        return;
    }

    if (values.some(v => v instanceof SubQuery)) {
        let subQueries = values.map(v => {
            if (v instanceof SubQuery) {
                params.push(...v.getParams());
                return v.toSQL();
            } else {
                params.push(v);
                return "?";
            }
        });
        conditions.push(`${field} IN (${subQueries.join(", ")})`);
    } else {
        if (operator === '$in') {
            if (values.length === 0) {
                conditions.push("1=0");
            } else {
                let placeholders = values.map(() => "?").join(", ");
                conditions.push(`${field} IN (${placeholders})`);
                params.push(...values);
            }
        } else if (operator === '$nin') {
            if (values.length === 0) {
                conditions.push("1=1");  // 修正：空 $nin 应匹配所有值
            } else {
                let placeholders = values.map(() => "?").join(", ");
                conditions.push(`${field} NOT IN (${placeholders})`);
                params.push(...values);
            }
        } else if (operator === '$all') {
            let subConds = values.map(val => {
                params.push(JSON.stringify(val));
                return `JSON_CONTAINS(${field}, ?)`;
            });
            conditions.push("(" + subConds.join(" AND ") + ")");
        }
    }

    Logger.debug(`数组操作符处理 (${context}) ${operator} for field ${field}:`, conditions[conditions.length - 1]);
}

function handleOperator(field, opValue, operator, conditions, params, context) {
    if (typeof opValue === 'object' && opValue !== null && opValue.$col) {
        const sqlOperator = OPERATORS_MYSQL[operator];
        if (!sqlOperator) {
            handleError(`Unsupported operator "${operator}" for field "${field}"`, context, SQLGenerationError);
        }
        conditions.push(`${field} ${sqlOperator} ${opValue.$col}`);
        Logger.debug(`字段引用处理 (${context}) ${operator} for field ${field}:`, conditions[conditions.length - 1]);
        return;
    }

    // 尝试使用插件处理
    for (const plugin of OperatorPlugins) {
        if (plugin.operator === operator) {
            plugin.apply(field, opValue, conditions, params, context);
            Logger.debug(`插件处理操作符 (${context}) ${operator} for field ${field}:`, conditions[conditions.length - 1]);
            return;
        }
    }
    if (opValue instanceof SubQuery) {
        const subSql = opValue.toSQL();
        conditions.push(`${field} IN ${subSql}`);
        params.push(...opValue.getParams());
        Logger.debug(`子查询处理 (${context}) for field ${field}:`, subSql);
        return;
    }
    switch (operator) {
        case "$exists":
            conditions.push(`${field} ${opValue ? "IS NOT NULL" : "IS NULL"}`);
            break;
        case '$size':
            conditions.push(`JSON_LENGTH(${field}) = ?`);
            params.push(opValue);
            break;
        case '$elemMatch':
            conditions.push(`JSON_CONTAINS(${field}, ?)`);
            params.push(JSON.stringify(opValue));
            break;
        case '$regex':
            conditions.push(`${field} REGEXP ?`);
            params.push(opValue);
            break;
        case '$like':
            conditions.push(`${field} LIKE ?`);
            params.push(opValue);
            break;
        case "$findInSet":
            conditions.push(`FIND_IN_SET(?, ${field}) > 0`);
            params.push(opValue);
            break;
        default:
            let sqlOperator = OPERATORS_MYSQL[operator];
            if (!sqlOperator) {
                handleError(`Unsupported operator "${operator}" for field "${field}"`, context, SQLGenerationError);
            }
            conditions.push(`${field} ${sqlOperator} ?`);
            params.push(opValue);
    }
    Logger.debug(`操作符处理 (${context}) ${operator} for field ${field}:`, conditions[conditions.length - 1]);
}

// ===================== 新增 handleCountStage 函数 =====================
/**
 * 处理 $count 阶段，生成用于统计分组数量的 SQL 片段。
 * @param {string} countField - 要输出的统计字段名，例如 "total"
 * @returns {Object} 包含 selectClause、groupByClause、havingClause 以及 _count 标识的对象
 */
function handleCountStage(countField) {
    return {
        selectClause: `COUNT(*) AS ${countField}`,
        groupByClause: "",    // 外层查询不需要 GROUP BY
        havingClause: "",
        _count: true          // 标记为 count 阶段
    };
}

// ===== 修改开始: 扩展 convertExpr 支持 $round、$ifNull、$divide =====
function convertExpr(expr) {
    // 如果表达式是 $cond，则转换成 SQL CASE WHEN 表达式
    if (typeof expr === "object" && expr !== null && expr.hasOwnProperty("$cond")) {
        const condArray = expr["$cond"];
        if (!Array.isArray(condArray) || condArray.length !== 3) {
            handleError("$cond 表达式必须是一个包含三个元素的数组", "convertExpr", SQLGenerationError);
        }
        const [condition, trueCase, falseCase] = condArray;
        const conditionSQL = convertExpr(condition);
        const trueSQL = convertExpr(trueCase);
        const falseSQL = convertExpr(falseCase);
        return `CASE WHEN ${conditionSQL} THEN ${trueSQL} ELSE ${falseSQL} END`;
    }

    // ===== 修改开始: 添加 $round 支持 =====
    if (typeof expr === "object" && expr !== null && expr.hasOwnProperty("$round")) {
        const roundArgs = expr["$round"];
        if (!Array.isArray(roundArgs) || roundArgs.length !== 2) {
            handleError("$round 操作符必须是一个包含两个元素的数组", "convertExpr", SQLGenerationError);
        }
        const innerExpr = convertExpr(roundArgs[0]);
        const precision = convertExpr(roundArgs[1]);
        return `ROUND(${innerExpr}, ${precision})`;
    }
    // ===== 修改结束: 添加 $round 支持 =====

    // ===== 修改开始: 添加 $ifNull 支持 =====
    if (typeof expr === "object" && expr !== null && expr.hasOwnProperty("$ifNull")) {
        const ifNullArgs = expr["$ifNull"];
        if (!Array.isArray(ifNullArgs) || ifNullArgs.length !== 2) {
            handleError("$ifNull 操作符必须是一个包含两个元素的数组", "convertExpr", SQLGenerationError);
        }
        const expr1 = convertExpr(ifNullArgs[0]);
        const expr2 = convertExpr(ifNullArgs[1]);
        return `COALESCE(${expr1}, ${expr2})`;
    }
    // ===== 修改结束: 添加 $ifNull 支持 =====

    // ===== 修改开始: 添加 $divide 支持 =====
    if (typeof expr === "object" && expr !== null && expr.hasOwnProperty("$divide")) {
        const divideArgs = expr["$divide"];
        if (!Array.isArray(divideArgs) || divideArgs.length !== 2) {
            handleError("$divide 操作符必须是一个包含两个元素的数组", "convertExpr", SQLGenerationError);
        }
        const numerator = convertExpr(divideArgs[0]);
        const denominator = convertExpr(divideArgs[1]);
        return `(${numerator} / ${denominator})`;
    }
    // ===== 修改结束: 添加 $divide 支持 =====

    // ===== 修改开始: 添加 $addToSet 支持 =====
    if (typeof expr === "object" && expr !== null && expr.hasOwnProperty("$addToSet")) {
        const addToSetArg = expr["$addToSet"];
        const inner = convertExpr(addToSetArg);
        return `JSON_ARRAYAGG(DISTINCT ${inner})`;
    }
    // ===== 修改结束: 添加 $addToSet 支持 =====

    // ===== 修改开始: 添加 $subtract 支持 =====
    if (typeof expr === "object" && expr !== null && expr.hasOwnProperty("$subtract")) {
        const subtractArgs = expr["$subtract"];
        if (!Array.isArray(subtractArgs) || subtractArgs.length !== 2) {
            handleError("$subtract 操作符必须是一个包含两个元素的数组", "convertExpr", SQLGenerationError);
        }
        const minuend = convertExpr(subtractArgs[0]);
        const subtrahend = convertExpr(subtractArgs[1]);
        return `(${minuend} - ${subtrahend})`;
    }
    // ===== 修改结束: 添加 $subtract 支持 =====

    // ===== 修改开始: 添加 $size 支持 =====
    if (typeof expr === "object" && expr !== null && expr.hasOwnProperty("$size")) {
        const sizeArg = expr["$size"];
        if (Array.isArray(sizeArg)) {
            if (sizeArg.length !== 1) {
                handleError("$size 操作符必须是一个包含一个元素的数组", "convertExpr", SQLGenerationError);
            }
            const inner = convertExpr(sizeArg[0]);
            return `JSON_LENGTH(${inner})`;
        } else {
            return `JSON_LENGTH(${convertExpr(sizeArg)})`;
        }
    }
    // ===== 修改结束: 添加 $size 支持 =====
    
    // ===== 修改开始: 添加 $add 支持 =====
if (typeof expr === "object" && expr !== null && expr.hasOwnProperty("$add")) {
    const addArgs = expr["$add"];
    if (!Array.isArray(addArgs) || addArgs.length < 2) {
        handleError("$add 操作符必须是一个包含至少两个元素的数组", "convertExpr", SQLGenerationError);
    }
    // 转换每个子表达式
    const convertedArgs = addArgs.map(item => convertExpr(item));
    // 拼接成 SQL 中的加法表达式
    return `(${convertedArgs.join(" + ")})`;
}
// ===== 修改结束: 添加 $add 支持 =====

    // 处理字符串：如果以 "$" 开头，认为是字段引用，去掉 "$"
    if (typeof expr === "string") {
        return expr.startsWith("$") ? expr.substring(1) : expr;
    }
    // 数字和布尔值直接转换为字符串
    if (typeof expr === "number" || typeof expr === "boolean") {
        return expr.toString();
    }
    // 对于其它类型，返回 JSON 字符串
    return JSON.stringify(expr);
}
// ===== 修改结束: 扩展 convertExpr 支持 $round、$ifNull、$divide =====




/**
 * 将排序条件转换成 SQL 排序子句字符串
 * 例如：{ id: -1, name: 1 } 转换为 "id DESC, name ASC"
 * @param {Object|string} sortClause 排序条件，可以是对象或者已经构造好的字符串
 * @returns {string} 转换后的排序字符串
 */
function transformSortClause(sortClause) {
    if (!sortClause) return "";

    // 如果传入的是对象，则遍历键值进行转换
    if (typeof sortClause === "object" && !Array.isArray(sortClause)) {
        const sortArr = [];
        for (const key in sortClause) {
            if (Object.hasOwnProperty.call(sortClause, key)) {
                let direction = sortClause[key];
                // 如果方向是数字，则 -1 表示 DESC，其他数字（比如 1）表示 ASC
                if (typeof direction === "number") {
                    sortArr.push(`${key} ${direction === -1 ? "DESC" : "ASC"}`);
                }
                // 如果方向是字符串，则将其转为大写后使用（假定传入 'asc' 或 'desc'）
                else if (typeof direction === "string") {
                    const upperDir = direction.toUpperCase();
                    if (upperDir === "DESC" || upperDir === "ASC") {
                        sortArr.push(`${key} ${upperDir}`);
                    } else {
                        // 如果传入的是其它值，直接忽略方向，或者抛出错误也可以
                        sortArr.push(`${key}`);
                    }
                }
                // 其它类型则忽略方向
                else {
                    sortArr.push(`${key}`);
                }
            }
        }
        return sortArr.join(", ");
    }

    // 如果传入的是字符串，则直接返回
    if (typeof sortClause === "string") {
        return sortClause;
    }

    return "";
}


/* ============================================================
   基本查询与 Join 查询生成函数
============================================================ */
/**
 * 基本的 SELECT 查询转换函数
 */
function mongoToMySQL(query, tableName, limit = 10, orderBy = 'id DESC') {
    let params = [];
    const whereClause = parseMongoQuery(query, params, "mongoToMySQL");
    const sql = `SELECT * FROM ${tableName} WHERE ${whereClause} ORDER BY ${orderBy} LIMIT ${limit}`;
    Logger.info("基本查询 SQL:", sql, "参数:", params);
    return { sql, params };
}

/**
 * 处理连表查询的辅助函数
 */
function prepareJoinMappings(joinConfigs) {
    const mappings = {};
    joinConfigs.forEach(join => {
        let alias = join.alias || join.tableName;
        mappings[alias] = {
            tableName: join.tableName,
            joinType: join.joinType,
            on: join.on,
            alias: alias
        };
    });
    Logger.debug("连表映射生成:", mappings);
    return mappings;
}

function parseProjectStageWithJoinsOptimized(projectFields, joinMappings, mainTableName) {
    let fields = [];
    projectFields.forEach(field => {
        // 如果字段中已包含 " AS "（不区分大小写），直接使用该字段
        if (field.toLowerCase().includes(" as ")) {
            fields.push(field);
        } else if (field.indexOf('.') !== -1) {
            let parts = field.split(".");
            let alias = parts[0];
            let col = parts[1];
            if (joinMappings[alias]) {
                // 默认生成别名格式为 alias_column
                fields.push(`${alias}.${col} AS ${alias}_${col}`);
            } else {
                fields.push(`${mainTableName}.${field} AS ${mainTableName}_${field}`);
            }
        } else {
            fields.push(`${mainTableName}.${field} AS ${mainTableName}_${field}`);
        }
    });
    const result = fields.join(", ");
    Logger.debug("生成的投影 SELECT 片段:", result);
    return result;
}
/* MODIFICATION END */

function generateJoinClause(joinConfigs) {
    const joinClause = joinConfigs.map(join => {
        join.joinType = join.joinType || 'LEFT JOIN';
        const aliasPart = join.alias ? (' AS ' + join.alias) : '';

        let onCondition = join.on;
        if (typeof onCondition === "object") {
            const conditions = Object.entries(onCondition).map(([localField, foreignField]) => {
                foreignField = foreignField.startsWith("$") ? foreignField.substring(1) : foreignField;
                return `${join.alias || join.tableName}.${foreignField} = ${localField}`;
            });
            onCondition = conditions.join(" AND ");
        }

        return ` ${join.joinType} ${join.tableName}${aliasPart} ON ${onCondition}`;
    }).join(" ");

    Logger.debug("生成的 JOIN 子句:", joinClause);
    return joinClause;
}

function mongoToMySQLWithJoinsOptimized(query, tableName, joinConfigs = [], limit = 10, orderBy = 'id DESC') {
    let params = [];
    const joinMappings = prepareJoinMappings(joinConfigs);
    let selectClause = "*";
    if (query.$project) {
        selectClause = parseProjectStageWithJoinsOptimized(query.$project, joinMappings, tableName);
        delete query.$project;
    }
    const whereClause = parseMongoQuery(query, params, "mongoToMySQLWithJoinsOptimized");
    const joinClause = generateJoinClause(joinConfigs);
    const sql = `SELECT ${selectClause} FROM ${tableName}${joinClause} WHERE ${whereClause} ORDER BY ${orderBy} LIMIT ${limit}`;
    Logger.info("连表查询 SQL:", sql, "参数:", params);
    return { sql, params };
}

/* ============================================================
   子查询支持
============================================================ */
class SubQuery {
    constructor(queryBuilder) {
        this.queryBuilder = queryBuilder;
        this.cachedSQL = null;
    }
    toSQL() {
        if (!this.cachedSQL) {
            const result = this.queryBuilder.toSQL();
            this.cachedSQL = "(" + result.sql + ")";
        }
        return this.cachedSQL;
    }
    getParams() {
        return this.queryBuilder.toSQL().params;
    }
}

/* ============================================================
   聚合查询支持
============================================================ */
function parseGroupStage(groupObj) {
    let selectParts = [];
    let groupByParts = [];
    let havingParts = [];

    if (groupObj._id === null) {
        selectParts.push(`NULL AS _id`);
    } else if (typeof groupObj._id === "object" && !Array.isArray(groupObj._id)) {
        let subFields = [];
        let groupFields = [];
        for (let key in groupObj._id) {
            let val = groupObj._id[key];
            if (typeof val === "string" && val.startsWith("$")) {
                val = val.substring(1);
            }
            subFields.push(`${val} AS ${key}`);
            groupFields.push(val);
        }
        selectParts.push(subFields.join(", "));
        groupByParts.push(groupFields.join(", "));
    } else if (typeof groupObj._id === "string" && groupObj._id.startsWith("$")) {
        const field = groupObj._id.substring(1);
        selectParts.push(`${field} AS _id`);
        groupByParts.push(field);
    } else {
        selectParts.push(`'${groupObj._id}' AS _id`);
        groupByParts.push("_id");
    }

    for (let key in groupObj) {
        if (key === "_id") continue;
        const operatorObj = groupObj[key];
        const operator = Object.keys(operatorObj)[0];
        const operand = operatorObj[operator];
        let sqlFunc = "";

        switch (operator) {
            case "$sum": sqlFunc = "SUM"; break;
            case "$avg": sqlFunc = "AVG"; break;
            case "$min": sqlFunc = "MIN"; break;
            case "$max": sqlFunc = "MAX"; break;
            case "$first": sqlFunc = "ANY_VALUE"; break;
            case "$last": sqlFunc = "ANY_VALUE"; break;
            case "$count": sqlFunc = "COUNT(*)"; break;
            case "$stdDevPop": sqlFunc = "STDDEV_POP"; break;
            case "$stdDevSamp": sqlFunc = "STDDEV_SAMP"; break;
            case "$addToSet": sqlFunc = "JSON_ARRAYAGG(DISTINCT"; break;
            case "$push": sqlFunc = "JSON_ARRAYAGG"; break;
            default:
                handleError(`不支持的聚合操作符: ${operator}`, "parseGroupStage", SQLGenerationError);
        }

        // ===== 修改开始: 添加 $cond 支持 =====
        let operandStr;
        if (typeof operand === "object" && operand !== null && operand.hasOwnProperty("$cond")) {
            operandStr = convertExpr(operand);
        } else if (typeof operand === "string" && operand.startsWith("$")) {
            operandStr = operand.substring(1);
        } else {
            operandStr = (operand !== undefined ? operand : "NULL");
        }
        // ===== 修改结束: 添加 $cond 支持 =====

        if (operator === "$count") {
            selectParts.push(`${sqlFunc} AS ${key}`);
        } else if (operator === "$addToSet" || operator === "$push") {
            selectParts.push(`${sqlFunc}(${operandStr})) AS ${key}`);
        } else {
            selectParts.push(`${sqlFunc}(${operandStr}) AS ${key}`);
        }
    }

    return {
        selectClause: selectParts.join(", "),
        groupByClause: groupByParts.join(", "),
        havingClause: havingParts.join(" AND ")
    };
}

class MongoAggregationBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.pipeline = [];
        this.joinConfigs = [];
        this.joinClause = []; // 用于 $lookup 生成 JOIN 子句
    }
    match(query) {
        this.pipeline.push({ $match: query });
        return this;
    }
    group(groupObj) {
        this.pipeline.push({ $group: groupObj });
        return this;
    }
    project(projection) {
        this.pipeline.push({ $project: projection });
        return this;
    }
    sort(sortObj) {
        this.pipeline.push({ $sort: sortObj });
        return this;
    }
    limit(n) {
        this.pipeline.push({ $limit: n });
        return this;
    }
    skip(n) {
        this.pipeline.push({ $skip: n });
        return this;
    }
    join(joinConfigs) {
        this.pipeline.push({ $join: joinConfigs });
        this.joinConfigs = joinConfigs;
        return this;
    }
    unwind(field) {
        this.pipeline.push({ $unwind: field });
        return this;
    }
    count(fieldName = "total") {
        this.pipeline.push({ $count: fieldName });
        return this;
    }

    /**
     * 在聚合管道中添加子查询阶段，
     * 该阶段将在 SELECT 子句中内嵌子查询，
     * 生成形如: (子查询SQL) AS fieldName
     *
     * @param {string} fieldName - 子查询结果的输出字段名
     * @param {Object} subQueryBuilder - 一个查询构造器实例，可以是 MongoQueryBuilder 或 MongoAggregationBuilder，
     *                                   需要使用 SubQuery 封装（如果还未封装，则内部自动封装）
     */
    subQuery(fieldName, subQueryBuilder) {
        // =====【修改开始】=====
        // 如果第一个参数不是字符串，则认为没有传入 fieldName，
        // 此时将第一个参数作为 subQueryBuilder，并将 fieldName 设为 undefined
        if (typeof fieldName !== 'string') {
            subQueryBuilder = fieldName;
            fieldName = undefined;
        }
        // 如果 fieldName 未传入，则默认使用子查询构造器中的表名作为别名
        if (!fieldName) {
            let defaultTableName;
            // 如果 subQueryBuilder 已经是 SubQuery 实例，则取其内部 queryBuilder 的 tableName
            if (subQueryBuilder instanceof SubQuery) {
                defaultTableName = subQueryBuilder.queryBuilder.tableName;
            } else if (subQueryBuilder.tableName) {
                // 如果 subQueryBuilder 是 MongoAggregationBuilder 实例，则直接取其 tableName 属性
                defaultTableName = subQueryBuilder.tableName;
            } else {
                defaultTableName = "subquery";
            }
            fieldName = defaultTableName;
        }
        // =====【修改结束】=====

        // 如果传入的不是 SubQuery 实例，则进行封装
        if (!(subQueryBuilder instanceof SubQuery)) {
            subQueryBuilder = new SubQuery(subQueryBuilder);
        }
        // 将子查询信息存入管道中，后续在 toSQL() 中处理
        this.pipeline.push({ $subquery: { field: fieldName, subquery: subQueryBuilder } });
        return this;
    }


    toSQL() {
        let params = [];
        let whereConditions = [];
        let groupClause = "";
        let havingClause = "";
        let selectClause = "*";
        let orderClause = "";
        let limitClause = "";
        let offsetClause = "";
        let countStage = null;
        let comments = []; // 用于保存 $unwind 等阶段的注释

        for (let stage of this.pipeline) {
            if (stage.$match) {
                let conditionStr = parseMongoQuery(stage.$match, params, "$match");
                if (conditionStr) {
                    whereConditions.push(conditionStr);
                }
            } else if (stage.$group) {
                const groupResult = parseGroupStage(stage.$group);
                selectClause = groupResult.selectClause;
                groupClause = groupResult.groupByClause;
                havingClause = groupResult.havingClause;
            } else if (stage.$project) {
                if (stage.$project && stage.$project.length > 0) {
                    if (this.joinConfigs && this.joinConfigs.length > 0) {
                        selectClause = parseProjectStageWithJoinsOptimized(stage.$project, prepareJoinMappings(this.joinConfigs), this.tableName);
                    }else{
                        if (Array.isArray(stage.$project)) {
                            if(selectClause === "*") {
                                selectClause = stage.$project.join(", ");
                            }else{
                                selectClause = `${stage.$project.join(", ")}, ${selectClause}`;
                            }
                        }
                    }
                }

            }else if (stage.$sort) {
                let sortArr = [];
                for (let key in stage.$sort) {
                    let direction = stage.$sort[key] === -1 ? "DESC" : "ASC";
                    sortArr.push(`${key} ${direction}`);
                }
                orderClause = sortArr.join(", ");
            } else if (stage.$limit) {
                limitClause = stage.$limit;
            } else if (stage.$skip) {
                offsetClause = stage.$skip;
            } else if (stage.$unwind) {
                comments.push(`/* UNWIND(${stage.$unwind}) */`);
            } else if (stage.$count) {
                // 调用 handleCountStage 处理 $count 阶段
                const countResult = handleCountStage(stage.$count);
                selectClause = countResult.selectClause;
                groupClause = countResult.groupByClause;
                havingClause = countResult.havingClause;
                if (countResult._count) {
                    countStage = true;
                }
            }else if (stage.$subquery) {
                // 从该阶段中取出字段名称和子查询对象
                const fieldName = stage.$subquery.field;
                // 调用子查询的 toSQL() 方法得到子查询 SQL
                const subSQL = stage.$subquery.subquery.toSQL();
                // 如果当前 selectClause 为 "*"，则直接使用子查询作为输出字段，
                // 否则将子查询拼接到已有的 selectClause 后面，形成多字段输出
                if (selectClause === "*") {
                    selectClause = `(${subSQL}) AS ${fieldName}`;
                } else {
                    selectClause += `, (${subSQL}) AS ${fieldName}`;
                }
            }
        }

        let sql = "SELECT " + selectClause + " FROM " + this.tableName;

        if (groupClause) {
            sql = "SELECT " + selectClause + " FROM " + this.tableName;
        }

        // 处理 JOIN 子句
        if (this.joinConfigs && this.joinConfigs.length > 0) {
            sql += generateJoinClause(this.joinConfigs);
        }

        if (comments.length > 0) {
            sql += " " + comments.join(" ");
        }
        if (whereConditions.length > 0) {
            sql += " WHERE " + whereConditions.join(" AND ");
        }
        if (groupClause) {
            sql += " GROUP BY " + groupClause;
        }
        if (havingClause) {
            sql += " HAVING " + havingClause;
        }
        if (orderClause) {
            sql += " ORDER BY " + orderClause;
        }
        if (limitClause) {
            sql += " LIMIT " + limitClause;
            if (offsetClause) {
                sql += " OFFSET " + offsetClause;
            }
        } else if (offsetClause) {
            sql += " LIMIT 18446744073709551615 OFFSET " + offsetClause;
        }

        Logger.info("生成的 SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

/* ============================================================
   更新、删除与新增支持
============================================================ */
class MongoUpdateBuilder {
    constructor(tableName, idField = "id") {
        this.tableName = tableName;
        this.idField = idField;
        this.dataObj = null;
        this.dataArray = null;
        this.filter = {};
        this.singleUpdate = false;
    }
    update(updateData) {
        if (Array.isArray(updateData)) {
            updateData.forEach(obj => {
                if (!obj.hasOwnProperty(this.idField)) {
                    handleError(`每个更新对象必须包含 '${this.idField}' 字段`, "MongoUpdateBuilder", SQLGenerationError);
                }
            });
            this.dataArray = updateData;
            this.dataObj = null;
        } else if (typeof updateData === 'object' && updateData !== null) {
            if (Object.keys(updateData).every(key => !key.startsWith('$'))) {
                this.dataObj = { $set: updateData };
            } else {
                this.dataObj = updateData;
            }
            this.dataArray = null;
        } else {
            handleError("更新数据必须为对象或数组。", "MongoUpdateBuilder", SQLGenerationError);
        }
        return this;
    }
    query(queryObj) {
        if (Object.keys(this.filter).length === 0) {
            this.filter = queryObj;
        } else {
            if (!this.filter.$and) {
                this.filter = { $and: [this.filter] };
            }
            this.filter.$and.push(queryObj);
        }
        return this;
    }
    single() {
        this.singleUpdate = true;
        return this;
    }
    toSQL() {
        if (Object.keys(this.filter).length === 0) {
            handleError("更新操作必须指定查询条件，防止误更新所有记录。", "MongoUpdateBuilder", SQLGenerationError);
        }
        let params = [];
        let sql = "";
        if (this.dataObj) {
            let updateOps = this.dataObj;
            let setClauses = [];
            for (let op in updateOps) {
                switch (op) {
                    case "$set":
                        if (!updateOps.$set || Object.keys(updateOps.$set).length === 0) {
                            handleError("无效的 $set 更新操作: 不能为空", "MongoUpdateBuilder.toSQL", SQLGenerationError);
                        }
                        for (let field in updateOps.$set) {
                            if (updateOps.$set[field] === undefined) {
                                handleError(`字段 ${field} 不能设置为 undefined`, "MongoUpdateBuilder.toSQL", SQLGenerationError);
                            }
                            setClauses.push(`${field} = ?`);
                            params.push(updateOps.$set[field]);
                        }
                        break;

                    case "$inc":
                        for (let field in updateOps.$inc) {
                            setClauses.push(`${field} = ${field} + ?`);
                            params.push(updateOps.$inc[field]);
                        }
                        break;
                    case "$unset":
                        if (typeof updateOps.$unset === "object") {
                            for (let field in updateOps.$unset) {
                                setClauses.push(`${field} = NULL`);
                            }
                        } else {
                            handleError("无效的 $unset 语法，应为 { field1: '', field2: '' }", "MongoUpdateBuilder", SQLGenerationError);
                        }
                        break;
                    case "$mul":
                        for (let field in updateOps.$mul) {
                            setClauses.push(`${field} = ${field} * ?`);
                            params.push(updateOps.$mul[field]);
                        }
                        break;
                    default:
                        handleError("不支持的更新操作符: " + op, "MongoUpdateBuilder", SQLGenerationError);
                }
            }
            if (setClauses.length === 0) {
                handleError("未指定更新字段。", "MongoUpdateBuilder", SQLGenerationError);
            }
            sql = `UPDATE ${this.tableName} SET ${setClauses.join(", ")}`;
            let whereClause = parseMongoQuery(this.filter, params, "UPDATE");
            sql += ` WHERE ${whereClause}`;
        } else if (this.dataArray) {
            if (this.dataArray.length === 0) {
                handleError("未指定更新对象。", "MongoUpdateBuilder", SQLGenerationError);
            }
            let updateColumns = new Set();
            this.dataArray.forEach(obj => {
                for (let key in obj) {
                    if (key !== this.idField) {
                        updateColumns.add(key);
                    }
                }
            });
            updateColumns = Array.from(updateColumns);
            if (updateColumns.length === 0) {
                handleError("批量更新时，未检测到有效字段。", "MongoUpdateBuilder", SQLGenerationError);
            }
            let setClauses = updateColumns.map(col => {
                let cases = this.dataArray.map(obj => {
                    if (obj.hasOwnProperty(col)) {
                        return `WHEN ? THEN ?`;
                    }
                    return "";
                }).filter(x => x !== "").join(" ");
                return `${col} = CASE ${this.idField} ${cases} ELSE ${col} END`;
            });
            let caseParams = [];
            updateColumns.forEach(col => {
                this.dataArray.forEach(obj => {
                    if (obj.hasOwnProperty(col)) {
                        caseParams.push(obj[this.idField]);
                        caseParams.push(obj[col]);
                    }
                });
            });
            const ids = this.dataArray.map(obj => obj[this.idField]);
            const placeholders = ids.map(() => "?").join(", ");
            let bulkWhereClause = `${this.idField} IN (${placeholders})`;
            let extraParams = [];
            let extraCondition = parseMongoQuery(this.filter, extraParams, "BULK_UPDATE");
            if (extraCondition !== "1=1") {
                bulkWhereClause += ` AND (${extraCondition})`;
            }
            sql = `UPDATE ${this.tableName} SET ${setClauses.join(", ")} WHERE ${bulkWhereClause}`;
            params = caseParams.concat(ids, extraParams);
        } else {
            handleError("未指定更新数据。", "MongoUpdateBuilder", SQLGenerationError);
        }
        if (this.singleUpdate) {
            sql += " LIMIT 1";
        }
        Logger.info("生成的 UPDATE SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

class MongoDeleteBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.filter = {};
        this.singleDelete = false;
    }
    query(queryObj) {
        if (Object.keys(this.filter).length === 0) {
            this.filter = queryObj;
        } else {
            if (!this.filter.$and) {
                this.filter = { $and: [this.filter] };
            }
            this.filter.$and.push(queryObj);
        }
        return this;
    }
    single() {
        this.singleDelete = true;
        return this;
    }
    toSQL() {
        if (!this.filter || Object.keys(this.filter).length === 0) {
            handleError("删除操作必须指定查询条件，防止误删除所有记录。", "MongoDeleteBuilder", SQLGenerationError);
        }
        let params = [];
        let whereClause = parseMongoQuery(this.filter, params, "DELETE");
        let sql = `DELETE FROM ${this.tableName} WHERE ${whereClause}`;
        if (this.singleDelete) {
            sql += " LIMIT 1";
        }
        Logger.info("生成的 DELETE SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

class MongoInsertBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.doc = null;
        this.docs = [];
        this.upsertEnabled = false;
        this.upsertFields = null;
    }
    insertOne(doc) {
        this.doc = doc;
        return this;
    }
    insertMany(docs) {
        this.docs = docs;
        return this;
    }
    upsert(enable = true, fields = null) {
        this.upsertEnabled = enable;
        this.upsertFields = fields;
        return this;
    }
    toSQL() {
        if (this.doc) {
            const keys = Object.keys(this.doc);
            const columns = keys.join(", ");
            const placeholders = keys.map(() => "?").join(", ");
            let sql = `INSERT INTO ${this.tableName} (${columns}) VALUES (${placeholders})`;
            let params = keys.map(key => this.doc[key]);
            if (this.upsertEnabled) {
                const updateFields = this.upsertFields || keys;
                const updateClause = updateFields.map(col => `${col} = VALUES(${col})`).join(", ");
                sql += ` ON DUPLICATE KEY UPDATE ${updateClause}`;
            }
            Logger.info("生成的单条 INSERT SQL:", sql, "参数:", params);
            return { sql, params };
        } else if (!Array.isArray(this.docs) || this.docs.length === 0) {
            handleError("insertMany() 需要至少一个有效的文档。", "MongoInsertBuilder", SQLGenerationError);
        } else{
            const keys = Object.keys(this.docs[0]);
            const columns = keys.join(", ");
            const rowPlaceholders = "(" + keys.map(() => "?").join(", ") + ")";
            let sql = `INSERT INTO ${this.tableName} (${columns}) VALUES ${this.docs.map(() => rowPlaceholders).join(", ")}`;
            let params = [];
            this.docs.forEach(doc => {
                keys.forEach(key => {
                    params.push(doc[key]);
                });
            });
            if (this.upsertEnabled) {
                const updateFields = this.upsertFields || keys;
                const updateClause = updateFields.map(col => `${col} = VALUES(${col})`).join(", ");
                sql += ` ON DUPLICATE KEY UPDATE ${updateClause}`;
            }
            Logger.info("生成的批量 INSERT SQL:", sql, "参数:", params);
            return { sql, params };
        }
    }
}

/* ============================================================
   MongoQueryBuilder 类：支持链式调用构造查询
============================================================ */
class MongoQueryBuilder {
    constructor(tableName) {
        this.tableName = tableName;
        this.filter = {};
        this.projection = null;
        this.joinConfigs = [];
        this.sortClause = "";
        this.limitValue = null;
        this.offsetValue = null;
    }

    query(queryObj) {
        if (Object.keys(this.filter).length === 0) {
            this.filter = queryObj;
        } else {
            if (!this.filter.$and) {
                this.filter = { $and: [this.filter] };
            }
            this.filter.$and.push(queryObj);
        }
        return this;
    }

    project(fields) {
        this.projection = fields;
        return this;
    }

    join(joinConfigs) {
        this.joinConfigs = joinConfigs;
        return this;
    }

    sort(sortBy) {
        this.sortClause = transformSortClause(sortBy);
        return this;
    }

    limit(limit) {
        this.limitValue = limit;
        return this;
    }

    skip(skip) {
        this.offsetValue = skip;
        return this;
    }

    count() {
        let params = [];
        let sql = `SELECT COUNT(*) AS total FROM ${this.tableName}`;
        if (this.joinConfigs && this.joinConfigs.length > 0) {
            sql += generateJoinClause(this.joinConfigs);
        }
        // 根据过滤条件生成 WHERE 子句
        const whereClause = parseMongoQuery(this.filter, params, "COUNT");
        if (whereClause) {
            sql += ` WHERE ${whereClause}`;
        }
        Logger.info("生成的 COUNT SQL:", sql, "参数:", params);
        return { sql, params };
    }

    toSQL() {
        let params = [];
        let selectClause = "*";
        if (this.projection && this.projection.length > 0) {
            if (this.joinConfigs && this.joinConfigs.length > 0) {
                selectClause = parseProjectStageWithJoinsOptimized(this.projection, prepareJoinMappings(this.joinConfigs), this.tableName);
            } else {
                if (Array.isArray(this.projection)) {
                    selectClause = this.projection.join(", ");
                }
            }
        }

        let sql = `SELECT ${selectClause} FROM ${this.tableName}`;

        // 处理 JOIN 子句
        if (this.joinConfigs && this.joinConfigs.length > 0) {
            sql += generateJoinClause(this.joinConfigs);
        }

        // 处理 WHERE 子句
        const whereClause = parseMongoQuery(this.filter, params, "SELECT");
        if (whereClause) {
            sql += ` WHERE ${whereClause}`;
        }

        // 处理排序
        if (this.sortClause) {
            sql += ` ORDER BY ${this.sortClause}`;
        }

        // 处理分页
        if (this.limitValue !== null) {
            sql += ` LIMIT ${this.limitValue}`;
            if (this.offsetValue !== null) {
                sql += ` OFFSET ${this.offsetValue}`;
            }
        } else if (this.offsetValue !== null) {
            sql += ` LIMIT 18446744073709551615 OFFSET ${this.offsetValue}`;
        }

        Logger.info("最终生成的 SELECT SQL:", sql, "参数:", params);
        return { sql, params };
    }
}

/* ============================================================
   模块导出
============================================================ */
module.exports = {
    MongoQueryBuilder,
    MongoAggregationBuilder,
    MongoUpdateBuilder,
    MongoDeleteBuilder,
    MongoInsertBuilder,
    SubQuery,
    registerOperatorPlugin,
    Logger,
    QueryParseError,
    SQLGenerationError,
    mongoToMySQL,
    mongoToMySQLWithJoinsOptimized
};
