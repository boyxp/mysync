package main

import "os"
import "log"
import "regexp"
import "path/filepath"
import "strings"
import "database/sql"
import "mysync/model"
import "encoding/json"
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

    //检查表是否存在
    if _, ok := tables[tmp[1]];ok==false {
      create_table(tmp[1], strings.Replace(file, "backup", "scheme", 1))
      tables[tmp[1]] = tmp[1]

      //如果存在，对比结构是否有差异
    } else {
      alter_table(tmp[1], strings.Replace(file, "backup", "scheme", 1))
    }

    log.Println(file)
  }

  /*
  读取备份文件内容，遍历
    如果是插入，则直接插入
    如果是更新，则删除主键后，按主键为条件更新
    更新恢复进度：最大ID、最后时间
  */
}


var field_type = map[string]string{
  "char":"text",
  "varchar":"text",
  "binary":"text",
  "varbinary":"text",
  "blob":"text",
  "text":"text",
  "enum":"text",
  "set":"text",
  "date":"time",
  "time":"time",
  "datetime":"time",
  "timestamp":"time",
  "year":"time",
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

  log.Println(_sql)


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
        if _t=="text" {
            _default = " DEFAULT '"+default_value+"'"
        } else if(_t=="time") {
            re, err := regexp.Compile("[0-9]")
            if err != nil {
              log.Fatal(err)
            }
            if re.MatchString(default_value) {
                _default = " DEFAULT '"+default_value+"'"
            } else {
                _default = " DEFAULT "+default_value
            }
        } else {
            _default = " DEFAULT "+default_value
        }
      }
    }

    return _default
}

func alter_table(table string, scheme string) {
  new_scheme := load_scheme(scheme)
  old_scheme := table_scheme(table)

  //log.Println(new_scheme)
  //log.Println(old_scheme)

  for _, new_field := range new_scheme {
    //新增字段
    if _, ok := old_scheme[new_field["field"]];ok==false {
          _type    := _field_type(new_field["type"])
          _null    := _field_null(new_field["null"])
          _default := _field_default(_type, new_field["default"])

          _sql := "ALTER TABLE "+table+" ADD `"+new_field["field"]+"` "+new_field["type"]+_null+" "+new_field["extra"]+" "+_default+";"
          log.Println(_sql)
      //新增字段
      continue
    }

    //旧字段对比差异
    old_field := old_scheme[new_field["field"]]
    if new_field["type"]!=old_field["type"] || new_field["null"]!=old_field["null"] || new_field["default"]!=old_field["default"] {
          _type    := _field_type(new_field["type"])
          _null    := _field_null(new_field["null"])
          _default := _field_default(_type, new_field["default"])

          _sql := "ALTER TABLE "+table+" CHANGE `"+old_field["field"]+"` `"+new_field["field"]+"` "+new_field["type"]+_null+" "+new_field["extra"]+" "+_default+";"
          log.Println(_sql, old_field, new_field)
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
