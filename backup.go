package main

import "os"
import "log"
import "strings"
import "encoding/json"
import "github.com/boyxp/nova/database"
import _ "github.com/joho/godotenv/autoload"
import "mysync/model"
import "database/sql"
import "bufio"

var db *sql.DB

func init() {
	database.Register("database", os.Getenv("database.dbname"), os.Getenv("database.dsn"))
}

func main() {
	model.Init()

	db = database.Open("database")

	include := os.Getenv("include")
	exclude := os.Getenv("exclude")

	log.Println(include, exclude)

	tables := table_list()
	for _,table := range tables {
		if include!="" && !strings.Contains(","+include+",", ","+table+",") {
			continue
		}

		if exclude!="" && strings.Contains(","+exclude+",", ","+table+",") {
			continue
		}

		scheme := table_scheme(table)
		save_scheme(table, scheme)
	}
}


/*
✅5、分析表的主键、创建时间和更新时间字段，插入首条记录，存储主键字段、更新时间字段、创建时间字段，最大ID为0
✅6、导出数据存储json，一个表一个文件，带最后备份时间，记录包含主键、类型（新增、修改、删除）、最后时间、数据内容json
✅7、存储每个表的进度，类型为备份、表名、最大主键，两个最大时间点。二次备份时先检查小于最大主键但更新的记录，然后全量读取大于该主键的记录
✅8、主库只有备份记录，如果执行恢复操作则禁止，执行前先检查，有恢复记录则停止执行
备份文件命名：库名_表名_最大主键ID_最后更新时间.mysql.data

*/

func table_list() []string {
	var result []string

	res, _ := db.Query("SHOW TABLES")

	var table string

	for res.Next() {
		res.Scan(&table)
    	result = append(result, table)
	}

	return result
}

func table_scheme(table string) []map[string]string {
		db        := database.Open("database")
		rows, err := db.Query("describe "+table)
		if err != nil {
    		panic(err.Error())
		}

		var scheme []map[string]string
		var rowField, rowType, rowNull, rowKey, rowExtra string
		var rowDefault sql.RawBytes
		for rows.Next() {
			if err := rows.Scan(&rowField, &rowType, &rowNull, &rowKey, &rowDefault, &rowExtra); err != nil {
        		panic(err.Error())
    		}

			scheme = append(scheme, map[string]string{
				"field"  : rowField,
				"type"   : rowType,
				"null"   : rowNull,
				"key"    : rowKey,
				"default": string(rowDefault),
				"extra"  : rowExtra,
			})
		}

		return scheme
}

func save_scheme(table string, scheme []map[string]string) {
	json, err := json.Marshal(scheme)
	if err != nil {
        panic(err.Error())
    }

    file, err := os.OpenFile("scheme."+table, os.O_WRONLY|os.O_CREATE, 0666)
    if err != nil {
        panic(err.Error())
    }

    defer file.Close()
    write := bufio.NewWriter(file)
    write.WriteString(string(json))
    write.Flush()
}
