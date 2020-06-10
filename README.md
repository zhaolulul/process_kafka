# process_kafka

本代码是从kafka消费数据，然后插入到pymysql将数据插入到数据库中
注意：
插入的时候最好使用"INSERT INTO investment_article values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"这种方法插入，这种方法虽然
使用的%s，但是插入之后应该是什么类型就是什么类型。
插入使用如下：
'INSERT INTO investment_article values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")'
也能插入成功，但是插入的全是字符串。