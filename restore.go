package main

import "os"
import "io"
import "log"
import "bufio"
import "regexp"
import "strings"
import "database/sql"
import "mysync/model"
import "encoding/json"
import "path/filepath"
import "github.com/boyxp/nova/database"
import _ "github.com/joho/godotenv/autoload"

var db *sql.DB
var restore_time string

func init() {
    database.Register("database", os.Getenv("database.dbname"), os.Getenv("database.dsn"))
}

func main() {
  //初始化
  model.Init()

  //检查是否主库
  check := model.Mysync.Where("type", "backup").Find()
  if check!=nil {
    log.Fatal("主库禁止恢复数据")
  }

  //连接数据库
  db = database.Open("database")

  //扫描备份文件，遍历
  list, err := filepath.Glob("backup.*.json")
  if err != nil {
    panic(err)
  }

  //读取全部表
  tables := table_list()

  for _,file := range list {
    //拆分表名、时间
    tmp := strings.Split(file, ".")
    if len(tmp)!=4 {
      log.Fatal("备份文件命名格式错误：", file)
    }

    table := tmp[1]

    //检查表是否存在
    if _, ok := tables[table];ok==false {
      create_table(table, strings.Replace(file, "backup", "scheme", 1))
      tables[table] = table

      //如果存在，对比结构是否有差异
    } else {
      alter_table(table, strings.Replace(file, "backup", "scheme", 1))
    }

    log.Println(file)

    //导入数据
    restore_data(table, file)
  }
}


func restore_data(table string, data_file string) {
  f, err := os.Open(data_file)
  if err != nil {
    log.Fatal(err)
  }
  defer f.Close()

  insert_count := 0
  update_count := 0
  r := bufio.NewReader(f)
  for {
    bytes, _, err := r.ReadLine()
    if err == io.EOF {
      break
    }

    if err != nil {
      log.Fatal(err)
    }

    line := string(bytes)
    data := strings.Split(line, "\t")

    _json := map[string]string{}
    _conn := database.Model{table}
    err   = json.Unmarshal([]byte(data[3]), &_json)
    if err != nil {
       log.Println("error:", err)
    }

    if data[0]=="insert" {
      log.Println("插入：", line)
      _conn.Insert(_json)
      insert_count++

    } else if data[0]=="update" {
      log.Println("更新：", line)
      _conn.Where(data[1]).Update(_json)
      update_count++

    } else {
      log.Fatal("暂不支持的类型", data)
    }

  }

  os.Rename(data_file, data_file+".ok")

  log.Println("恢复完毕：", data_file, "插入：", insert_count, "更新：", update_count)
}

var field_type = map[string]string{
  "char"     : "text",
  "varchar"  : "text",
  "binary"   : "text",
  "varbinary": "text",
  "blob"     : "text",
  "text"     : "text",
  "enum"     : "text",
  "set"      : "text",
  "date"     : "time",
  "time"     : "time",
  "datetime" : "time",
  "timestamp": "time",
  "year"     : "time",
}
func create_table(table string, scheme string) {
  rows := load_scheme(scheme)


  _key := ""
  _sql := "CREATE TABLE "+table+"("
  for _, field := range rows {

    _type    := _field_type(field["type"])
    _null    := _field_null(field["null"])
    _default := _field_default(_type, field["default"])

    if field["key"]=="PRI" {
      _key = field["field"]
    }

    _sql = _sql+" `"+field["field"]+"` "+field["type"]+_null+" "+field["extra"]+" "+_default+","

  }

  if _key!="" {
    _sql = _sql+" PRIMARY KEY(`"+_key+"`)"
  }

  _sql = _sql +") ENGINE=InnoDB DEFAULT CHARSET=utf8"

  log.Println("建表：", table, "\tSQL:", _sql)

  sql_execute(_sql)
}

func _field_null(null string) string {
  if null=="NO" {
      return " NOT NULL"
  }

  return " NULL"
}

func _field_type(field_type string) string {
    _idx  := strings.Index(field_type, "(")
    if _idx>-1 {
      field_type = field_type[0:_idx]
    }

    return field_type
}

func _field_default(_type string, default_value string) string {
    _default := ""
    if default_value!="" {
      if _t, ok := field_type[_type];ok {
          switch _t {
            case "text":
                      _default = " DEFAULT '"+default_value+"'"
            case "time":
                      re, err := regexp.Compile("[0-9]")
                      if err != nil {
                        log.Fatal(err)
                      }
                      if re.MatchString(default_value) {
                          _default = " DEFAULT '"+default_value+"'"
                      } else {
                          _default = " DEFAULT "+default_value
                      }
            default :
                    _default = " DEFAULT "+default_value
          }
        } else {
          _default = " DEFAULT "+default_value
        }
    }

    return _default
}

func alter_table(table string, scheme string) {
  new_scheme := load_scheme(scheme)
  old_scheme := table_scheme(table)

  for _, new_field := range new_scheme {
    //新增字段
    if _, ok := old_scheme[new_field["field"]];ok==false {
          _type    := _field_type(new_field["type"])
          _null    := _field_null(new_field["null"])
          _default := _field_default(_type, new_field["default"])

          _sql := "ALTER TABLE "+table+" ADD `"+new_field["field"]+"` "+new_field["type"]+_null+" "+new_field["extra"]+" "+_default+";"

          sql_execute(_sql)
          log.Println("添加字段：", table, "\tSQL:", _sql)

          continue
    }

    //旧字段对比差异
    old_field := old_scheme[new_field["field"]]
    if new_field["type"]!=old_field["type"] || new_field["null"]!=old_field["null"] || new_field["default"]!=old_field["default"] {
          _type    := _field_type(new_field["type"])
          _null    := _field_null(new_field["null"])
          _default := _field_default(_type, new_field["default"])

          _sql := "ALTER TABLE "+table+" CHANGE `"+old_field["field"]+"` `"+new_field["field"]+"` "+new_field["type"]+_null+" "+new_field["extra"]+" "+_default+";"

          sql_execute(_sql)
          log.Println("修改字段：", table, "\tSQL:", _sql)
    }
  }
}

func table_scheme(table string) map[string]map[string]string {
    rows, err := db.Query("describe "+table)
    if err != nil {
        panic(err.Error())
    }

    scheme := map[string]map[string]string{}
    var rowField, rowType, rowNull, rowKey, rowExtra string
    var rowDefault sql.RawBytes
    for rows.Next() {
      if err := rows.Scan(&rowField, &rowType, &rowNull, &rowKey, &rowDefault, &rowExtra); err != nil {
            panic(err.Error())
        }

      scheme[rowField] = map[string]string{
        "field"  : rowField,
        "type"   : rowType,
        "null"   : rowNull,
        "key"    : rowKey,
        "default": string(rowDefault),
        "extra"  : rowExtra,
      }
    }

    return scheme
}

func sql_execute(_sql string) {
  stmt, err := db.Prepare(_sql)
  if err != nil {
    panic(err.Error())
  }
  defer stmt.Close()

  res, err := stmt.Exec()
  if err != nil {
    panic(err.Error())
  }

  log.Println(res)
}

func load_scheme(scheme string) []map[string]string {
  _, err := os.Stat(scheme)
  if err != nil && os.IsNotExist(err) {
      log.Fatal("表结构文件不存在：", scheme)
  }

  content, err := os.ReadFile(scheme)
  if err != nil {
      log.Fatal(err)
  }

  var rows []map[string]string
  err = json.Unmarshal(content, &rows)
  if err != nil {
    log.Fatal(err)
  }

  return rows
}

func table_list() map[string]string {
  result := map[string]string{}

  res, _ := db.Query("SHOW TABLES")

  var table string

  for res.Next() {
    res.Scan(&table)
    result[table] = table
  }

  return result
}
