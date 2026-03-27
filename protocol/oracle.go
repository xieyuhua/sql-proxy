package protocol

import (
	"fmt"
	"regexp"
// 	"log"
	"strings"
	"github.com/axgle/mahonia"
)

const (
	TNS_TYPE_EXECUTE   = 0x06
	TNS_TYPE_FETCH     = 0x04
	TNS_TYPE_LOGIN     = 0x01
)
//中文乱码
func fixChineseEncoding(input string) string {
    decoder := mahonia.NewDecoder("gbk")
    input = decoder.ConvertString(input)
    return input
}
// 判断是否是 SQL 请求包
func IsOracleSQLPacket(pkt []byte) bool {
	if len(pkt) < 8 {
		return false
	}
	pktType := pkt[4]
	return pktType == TNS_TYPE_EXECUTE || pktType == TNS_TYPE_FETCH
}

// 解析TCP负载中的Oracle SQL语句
func ParseOracleSQL(client, server string, payloadStr []byte) string {
    
    
	sql := regexp.MustCompile(`[\s\r\n]+`).ReplaceAllString(string(payloadStr), " ")
    //中文乱码
    sql = fixChineseEncoding(sql)
    sql = strings.ReplaceAll(sql, "@", "")
    sql = strings.ReplaceAll(sql, "�", "")
    sql = strings.ReplaceAll(sql, "?", "")
    sql = strings.TrimSpace(sql)
    
    
    verboseStr := ""
	re := regexp.MustCompile(`(?i)\b(@|\s|NULL|OR|THEN|END|ELSE|=|ON|SELECT|INSERT|UPDATE|DELETE|AND|TO_DATE|WHERE|DESC|ASC|BY|GROUP|FROM|numrow|JOIN|UNION|COMMIT|ROLLBACK|GRANT|REVOKE|INDEX|VIEW|TRIGGER|HAVING|DROP|ALTER|CREATE)\b.*`)
	matches := re.FindAllString(sql, -1)


	for _, match := range matches {
	    sql = strings.TrimSpace(match)
	    sql = strings.ReplaceAll(sql, "@", "")
		verboseStr = fmt.Sprintf("From %s To %s;  %s \n\n", client, server, sql)
// 		Log.Infof("sql: \n", verboseStr)
		fmt.Print(verboseStr)
	}

	return verboseStr
}
