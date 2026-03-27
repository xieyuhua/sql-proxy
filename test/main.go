package main

import (
	"fmt"
	"regexp"
	"strings"
)

func main() {
	// 模拟TCP数据包
	tcpData := `SELECT A .busno AS busno, SUM ( ROUND ( ( A .netprice * A .wareqty * A . TIMES + A .minqty * A . TIMES * A .minprice ), 6 ) * A .pile / 100 ) AS pile, NVL ( v_busno_class_set_03大.classname, '未划分' ) AS compname, NVL ( v_busno_class_set_03小.classname, '未划分' ) AS area, f_get_orgname (A .busno) AS orgname, SUM ( ROUND ( ( A .netprice * A .wareqty * A . TIMES + A .minqty * A . TIMES * A .minprice ), 2 ) ) AS netsum, COUNT (DISTINCT A .saleno) AS kll, COUNT (A .saleno) AS xscs, 1 AS days, SUM ( ROUND ( NVL ( ROUND ( ( ( CASE WHEN b.limitprice = 0 OR b.limitprice IS NULL THEN A .purprice ELSE b.limitprice END ) * ( A .wareqty + ( CASE WHEN A .stdtomin = 0 THEN 0 ELSE A .minqty / A .stdtomin END ) ) * A . TIMES ), 6 ), ROUND ( ( i.purprice * ( A .wareqty + ( CASE WHEN A .stdtomin
= 0 THEN 0 ELSE A .minqty / A .stdtomin END ) ) * A . TIMES ), 6 ) ), 6 ) ) AS puramt FROM t_area r1, t_factory f, t_sale_d A LEFT JOIN t_store_i i ON A .wareid = i.wareid AND A .batid = i.batid, t_sale_h c, t_ware b, v_busno_class_set_big v_busno_class_set_03大, v_busno_class_set_mid v_busno_class_set_03中, v_busno_class_set v_busno_class_set_03小 WHERE b.wareid = A .wareid AND b.compid = c.compid AND c.saleno = A .saleno AND f.factoryid = b.factoryid AND r1.areacode = b.areacode AND A .busno = c.busno AND A .accdate = c.accdate AND b.warekind <> 3 AND v_busno_class_set_03大.classgroupno = '03' AND v_busno_class_set_03大.BUSNO = c.busno AND v_busno_class_set_03大.compid = 2 AND v_busno_class_set_03中.classgroupno = '03' AND v_busno_class_set_03中.BUSNO = c.busno AND v_busno_class_set_03中.compid = 2 AND v_busno_class_set_03小.classgroupno = '03' AND v_busno_class_set_03小.BUSNO = c.busno AND v_busno_class_set_03小.compid = 2 AND A .saler <> 802 AND ( A .accdate = TO_DATE ('2023-08-10', 'yyyy-MM-dd') ) GROUP BY A .busno, NVL ( v_busno_class_set_03大.classname, '未划分' ), NVL ( v_busno_class_set_03中.classname, '未划分' ), NVL ( v_busno_class_set_03小.classname, '未划分' ) `

// 使用正则表达式去除空格和换行符
	resss := regexp.MustCompile(`[\s\r\n]+`)
	sql := resss.ReplaceAllString(string(tcpData), " ")


	// 使用正则表达式提取表名
	re := regexp.MustCompile(`(?i)\b(?:(?:FROM|JOIN)\s+([\w\.]+)\b|(\w+)\s*,?)`)
	matches := re.FindAllStringSubmatch(sql, -1)

	// 存储所有表名
	tableNames := make(map[string]bool)

	// 遍历匹配的表名
	for _, match := range matches {
		for i := 1; i < len(match); i++ {
			if match[i] != "" {
				tableName := strings.Trim(match[i], ", \n\r\t")
				// 如果表名包含空格，则只取空格前的部分
				if strings.Contains(tableName, " ") {
					tableName = strings.Split(tableName, " ")[0]
				}
				// 检查表名是否已经存在，避免重复添加
				if !tableNames[tableName] {
					tableNames[tableName] = true
				}
			}
		}
	}

	// 输出所有表名
	for tableName := range tableNames {
		fmt.Println("Table Name:", tableName)
	}
}