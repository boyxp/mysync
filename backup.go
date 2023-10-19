package main

import "os"
import "log"
import "time"
import "bufio"
import "strings"
import "database/sql"
import "encoding/json"
import "mysync/model"
import "github.com/boyxp/nova/database"
import _time "github.com/boyxp/nova/time"
import _ "github.com/joho/godotenv/autoload"

var db *sql.DB
var backup_time string

func init() {
	database.Register("database", os.Getenv("database.dbname"), os.Getenv("database.dsn"))
}

func main() {
	model.Init()

	//检查是否备库
	check := model.Mysync.Where("type", "restore").Find()
	if check!=nil {
		log.Fatal("备库无法二次备份")
	}

	//读取环境变量设置
	include := os.Getenv("include")
	exclude := os.Getenv("exclude")
	scheme  := os.Getenv("scheme")

	log.Println("备份包含：", include)
	log.Println("备份排除：", exclude)

	//连接数据库
	db = database.Open("database")

	//先休息一秒，避免备份同一秒有更新
	time.Sleep(1*time.Second)

	//统一备份时间
	backup_time = _time.Date("Y-m-d H:i:s")

	//遍历表备份
	tables := table_list()
	for _,table := range tables {
		if include!="" && !strings.Contains(","+include+",", ","+table+",") {
			continue
		}

		if exclude!="" && strings.Contains(","+exclude+",", ","+table+",") {
			continue
		}

		log.Println("开始备份：", table)

		_scheme := table_scheme(table)
		if scheme=="yes" {
			save_scheme(table, _scheme)
		}

		save_record(table, _scheme)
		backup_data(table)
		log.Println("完成备份：", table)
	}
}

func backup_data(table string) {
	//读取当前进度记录
	info := model.Mysync.Where("table_name", table).Find()
	if info==nil {
		log.Fatal("记录进度读取失败：", table)
	}

	//先检查是否有需要备份的记录
	check_new    := database.Model{table}.Where(info["pkey_field"], ">", info["latest_id"]).Count()
	check_update := database.Model{table}.Where(info["pkey_field"], "<=", info["latest_id"]).
					Where(info["update_field"], ">", info["latest_time"]).Count()
	if check_new==0 && check_update==0 {
		log.Println("没有需要备份的数据：", table)
		return
	}

	log.Println("上次备份最大主键：", info["latest_id"])
	log.Println("上次备份最大时间：", info["latest_time"])


	//打开备份文件
    file, err := os.OpenFile("backup."+table+"."+strings.Replace(backup_time, " ", "@", 1)+".json", os.O_WRONLY|os.O_CREATE, 0666)
    if err != nil {
        log.Fatal("备份文件创建失败：", file, err.Error())
    }
    defer file.Close()
    write := bufio.NewWriter(file)


	//先读取主键大于上次值的所有记录，备份类型为新建
	count_new := 0
	max_id := info["latest_id"]
	pkey   := info["pkey_field"]
	num    := 1000
	for true {
		list := database.Model{table}.Where(pkey, ">", max_id).
				Order(pkey, "asc").
				Limit(num).
				Select()

		for _, v := range list {
			count_new++
			max_id = v[pkey]
			_json, err := json.Marshal(v)
			if err != nil {
        		panic(err.Error())
   			}

			write.WriteString("insert\t"+v[pkey]+"\t"+v[info["create_field"]]+"\t"+string(_json)+"\n")
		}

		if len(list)<num {
			break
		}

		time.Sleep(1*time.Second)
	}

	log.Println("备份新增记录：", count_new)

	//再读取主键小于上次值的但更新时间大于上次值的记录，备份类型为更新
	count_update := 0
	tmp_id := "-1"
	latest_time := info["latest_time"]
	for true {
		list := database.Model{table}.Where(pkey, "<=", info["latest_id"]).
				Where(info["update_field"], ">", latest_time).
				Where(info["update_field"], "<=", backup_time).
				Where(pkey, ">", tmp_id).
				Order(pkey, "asc").
				Limit(num).
				Select()

		for _, v := range list {
			count_update++
			tmp_id = v[pkey]
			_json, err := json.Marshal(v)
			if err != nil {
        		panic(err.Error())
   			}

			write.WriteString("update\t"+v[pkey]+"\t"+v[info["update_field"]]+"\t"+string(_json)+"\n")
		}

		if len(list)<num {
			break
		}

		time.Sleep(1*time.Second)
	}

	log.Println("备份更新记录：", count_update)

	//写入文件
	write.Flush()

	//存储本次备份进度
	model.Mysync.Where("table_name", table).Update(map[string]string{
		"latest_id"   : max_id,
		"latest_time" : backup_time,
	})
}

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

    file, err := os.OpenFile("scheme."+table+"."+strings.Replace(backup_time, " ", "@", 1)+".json", os.O_WRONLY|os.O_CREATE, 0666)
    if err != nil {
        log.Fatal("结构文件创建失败：", file, err.Error())
    }

    defer file.Close()
    write := bufio.NewWriter(file)
    write.WriteString(string(json))
    write.Flush()
}

func table_field(scheme []map[string]string) map[string]string {
	result := map[string]string{"pkey_field":"","update_field":"","create_field":""}
	for _, f := range scheme {
		if f["key"]=="PRI" {
			result["pkey_field"] = f["field"]
		}

		if f["type"]=="timestamp" && strings.Contains(f["extra"],"CURRENT_TIMESTAMP") {
			result["update_field"] = f["field"]
		}

		if f["type"]=="timestamp" && f["default"]=="CURRENT_TIMESTAMP" {
			result["create_field"] = f["field"]
		}
	}

	return result
}

func save_record(table string, scheme []map[string]string) {
	fields := table_field(scheme)

	exist := model.Mysync.Where("table_name", table).Find()

	if exist!=nil {
		model.Mysync.Where("table_name", table).Update(map[string]string{
			"pkey_field"  : fields["pkey_field"],
			"create_field": fields["create_field"],
			"update_field": fields["update_field"],
		})

	} else {
		model.Mysync.Insert(map[string]string{
			"type"        : "backup",
			"table_name"  : table,
			"pkey_field"  : fields["pkey_field"],
			"create_field": fields["create_field"],
			"update_field": fields["update_field"],
			"latest_id"   : "-1",
			"record_count": "0",
		})
	}
}
