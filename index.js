'use strict';
const fs = require('fs');
const assert = require('assert');
const logger = require('tracer-logger');
const xml2js = require('xml2js');
const art = require('art-template');
const util = require('util');
const path = require('path');
const walk = require('walk');
const TemplateUtils = {
    join(array, sp = ',', name = 'arr') {
        let result = [];
        array.forEach((item, index) => result.push(`:_${name}_${index}`));
        return result.join(sp);
    },

    arrayToObj(array, name = 'arr') {
        let result = {};
        array.forEach((item, index) => result[`_${name}_${index}`] = item);
        return result;
    },

    fieldAttributeMap(model) {
        let result = [];
        Reflect.ownKeys(model.fieldRawAttributesMap).forEach(key => result.push(`${key} as ${model.fieldRawAttributesMap[key].fieldName || key}`));
        return result.join(',');
    }
};
art.defaults.imports.Utils = TemplateUtils;
art.defaults.imports.Model = {};
const Types = ['SELECT', 'RAW'];
class SequelizeDao {
    constructor(sequelize, model, template) {
        this.sequelize = sequelize;
        this.Model = sequelize.model(model);
        art.defaults.imports.Model[model] = this.Model;
        this.mapper = null;
        if (template) {
            xml2js.parseString(fs.readFileSync(template), {explicitArray: false, trim: true, mergeAttrs: true, explicitRoot: false}, async (err, res) => {
                if (err) console.error(err.stack);
            if (res) {
                await this.parseTemplate(res);
            } else {
                this.mapper = false;
            }
        });
        } else {
            this.mapper = false;
        }
    }

    /**
     * 保存实体
     * @param record
     * @param fields
     * @returns {Promise.<*>}
     */
    async save(record, fields) {
        Reflect.ownKeys(record).forEach(key => {record[key] === null && Reflect.deleteProperty(record, key)});
        return await this.Model.create(record, {fields});
    }

    /**
     * 根据ID查询
     * @param id
     * @returns {Promise.<void>}
     */
    async selectById(id) {
        return await this.Model.findById(id);
    }

    /**
     * 查询单个
     * @param where
     * @returns {Promise.<*>}
     */
    async findOne(where) {
        return await this.Model.findOne({where});
    }

    async findAll(where) {
        return await this.Model.findAll({where});
    }

    /**
     * 更新数据
     * @param record
     * @param where
     * @param fields
     * @returns {Promise.<Array.<affectedCount, affectedRows>>}
     */
    async update(record, where = {}, fields) {
        return await this.Model.update(record, {fields, where});
    }

    /**
     * 删除
     * @param where
     * @returns {Promise.<Integer>}
     */
    async remove(where = {}) {
        return await this.Model.destroy({where});
    }

    /**
     * 分页查询
     * @param sql
     * @param paging
     * @param params
     * @returns {Promise.<*>}
     */
    async selectByPage(sql, paging, params) {
        sql = sql.toLowerCase();
        let countSql = sql.replace(/select([\s\S]*)from/, 'select count(1) `count` from ');
        let orderByIndex = countSql.indexOf('order by');
        if (orderByIndex >= 0 && countSql.indexOf('?', orderByIndex) === -1) {
            countSql = countSql.substring(0, orderByIndex);
        }
        let countResult = await this.execSql(countSql, this.sequelize.QueryTypes.SELECT, params);
        let count = countResult[0]['count'];
        paging.count = count;
        if (count && count > 0) {
            paging.items = await this.execSql(`${sql} limit ${paging.index},${paging.size}`, this.sequelize.QueryTypes.SELECT, params);
        }
        return paging;
    }

    /**
     * 执行SQL
     * @param sql
     * @param type
     * @param params
     * @param model
     * @returns {Promise.<*>}
     */
    async execSql(sql, type, params, model) {
        assert(this.sequelize.QueryTypes[type], `不支持的类型 ${type}`);
        sql = sql.replace(/\n/g, ' ').replace(/\s+/g," ").replace(/\s+\(/,"(").replace(/\s+\)/,")");
        logger.log(`exec ${sql}`);
        return await this.sequelize.query(sql, {replacements: params, type, model});
    }

    /**
     * 调用模板函数
     * @param name
     * @param params
     * @param model
     * @returns {Promise.<*>}
     */
    async template(name, params, model) {
        let mapper = await this.getMapper();
        assert(mapper[name], `模板没有找到 ${name} 这个方法`);
        let sql = mapper[name].render(params);
        let result = await this.execSql(sql, mapper[name].type, params, model);
        if ((mapper[name].single === true || mapper[name].type === 'RAW') && Array.isArray(result)) {
            return result[0];
        }
        return result;
    }

    /**
     * 调用模板分页
     * @param name
     * @param paging
     * @param params
     * @returns {Promise.<*>}
     */
    async templateByPage(name, paging, params) {
        let mapper = await this.getMapper();
        assert(mapper[name], `模板没有找到 ${name} 这个方法`);
        assert(mapper[name].type === 'SELECT', `模板 ${name} 不是查询类型`);
        let sql = mapper[name].render(params);
        return this.selectByPage(sql, paging, params);
    }

    async getMapper() {
        if (this.mapper === null) {
            SequelizeDao.sleep(500);
            return await this.getMapper();
        }
        if (this.mapper === false)
            throw new Error(`no template`);
        return this.mapper;
    }

    async parseTemplate(res) {
        try {
            this.mapper = {};
            logger.log(`加载模板: ${util.inspect(res)}`);
            Reflect.ownKeys(res).forEach(key => {
                let type = key.toUpperCase();
            if (!Types.includes(type)) {
                logger.warn(`template not support type ${key}`);
                return;
            }
            let items = res[key];
            if (!Array.isArray(items)) items = [items];

            items.forEach(item => this.mapper[item.id] = {
                type,
                render: art.compile(item._),
                single: !!item.single
            });
        });
        } catch (e) {
            logger.error('解析模板异常', e);
        }
    }

    static sleep(n) {
        return new Promise(resolve => {
            setTimeout(() => resolve(), n);
        });
    }

    static objVal2Array(obj) {
        let array = [];
        Reflect.ownKeys(obj).forEach(key => {array.push(obj[key])});
        return array;
    }

    static load(root, fileStat) {
        let base = path.join(root, fileStat.name);
        if(fileStat.name.endsWith('.js')) {
            let pwd = path.relative(__dirname, base);
            if (!pwd.startsWith('.') && !pwd.startsWith('/')) {
                pwd = './' + pwd;
            }
            let indexOf = base.indexOf(':');
            if (!base.startsWith('/') && indexOf !== -1) {
                base = base.substring(0, indexOf).toUpperCase() + base.substring(indexOf);
            }
            return {
                path: pwd,
                name: fileStat.name,
                basePath: base,
                object: require.cache[base] || require(pwd)
            };
        }
        return {};
    }

    static loadEntity(sequelize, root, filter) {
        let walker = walk.walk(root, {
            followLinks: true,
            filters: filter || ['node_modules']
        });
        return new Promise(ok => {
            walker.on("file", async (root, fileStat, next) => {
                try {
                    let result = SequelizeDao.load(root, fileStat);
                    let model = result.object;
                    if (typeof model === 'function') {
                        let Entity = sequelize.import(result.path);
                        await Entity.sync({force: false});
                    }
                } catch (err) {
                    logger.error('加载Entity:%s:%s异常.', fileStat.name, root, err);
                }
                next();
            });
            walker.on("errors", (root, nodeStatsArray, next) => {
                nodeStatsArray.forEach(n => {
                    logger.error("[ERROR] ", n);
                });
                next();
            });
            walker.on("end", () => {
                logger.info('文件Entity加载完成!');
                ok();
            });
        });
    }
}
module.exports = SequelizeDao;
module.exports.Paging = require('./Paging');
module.exports.TemplateUtils = TemplateUtils;