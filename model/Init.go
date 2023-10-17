package model

import "strings"
import "github.com/boyxp/nova/database"

func Init() {
	db     := database.Open("database")
	_, err := db.Query("describe mysync")
	if err != nil {
		if strings.Contains(err.Error(), "exist") {
			sql := `CREATE TABLE mysync (
				id int NOT NULL AUTO_INCREMENT COMMENT '主键ID',
				type char(7) NOT NULL COMMENT '操作类型：backup/restore',
				table_name varchar(50) NOT NULL COMMENT '表名',
				pkey_field varchar(50) NOT NULL COMMENT '主键字段',
				create_field varchar(50) NOT NULL COMMENT '创建时间字段',
				update_field varchar(50) NOT NULL COMMENT '更新时间字段',
				latest_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最后时间',
      			latest_id bigint NOT NULL COMMENT '最后主键ID',
        		record_count bigint NOT NULL COMMENT '距离上次记录条数',
				create_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
				update_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录修改时间',
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8`

			_, err := db.Exec(sql)
			if err != nil {
				panic(err.Error())
			}
		}
	}
}
