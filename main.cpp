#include <iostream>
#include <occi.h>
#include <string.h>


using namespace std;
using namespace oracle::occi;


//Oracle Environment
Environment* env;


//Oracle Database connection settings
struct DBSetting
{
public:
    string user = "karl";
    string password = "doenitz";
    string server = "127.0.0.1:1521/orcl";
}dbSetting;


/*
* execute the sql query;include insert, update and delete
* @param：connection, the connection to the oracle database；sql, the sql query
* @return：true or false
* */
bool execute_sql(Connection *connection, char *sql)
{
    try {
        Statement* stmt = connection->createStatement(sql);
        ResultSet* rs = stmt->executeQuery();
        stmt->closeResultSet(rs);
        connection->terminateStatement(stmt);
        return true;
    }
    catch(SQLException sqlException)
    {
        cout << "execute sql error:" << sql << endl;
        return false;
    }
}


/*
* execute several sql query;include insert, update and delete
* @param：connection, the connection to the oracle database；sql array, the sql query
* @return：true or false
* */
bool execute(char *sql[], int length)
{
    try {
        Connection* con = env->createConnection(dbSetting.user, dbSetting.password, dbSetting.server);
        for(int i=0; i< length; i++)
        {
            bool result = execute_sql(con, sql[i]);
            if(!result)
                return false;
        }
        env->terminateConnection(con);
        return true;
    }
    catch(ORAEXCEPTION exception)
    {
        cout << "create connection failed!" << endl;
        return false;
    }
}



/*
* execute the select query;
* @param: connection, the connection to the oracle database; sql, the sql query
* @return: a json type string
* */
char *select_sql(char *sql)
{
    Connection* con = env->createConnection(dbSetting.user, dbSetting.password, dbSetting.server);
    Statement* stmt = con->createStatement(sql);
    ResultSet* rs = stmt->executeQuery();
    OCCI_STD_NAMESPACE::vector<MetaData> metaData = rs->getColumnListMetaData();
    long size = metaData.size();
    string result = "[";
    while(rs->next()){
        for(unsigned int index=0; index<size; index++)
        {
            if(size > 1)
            {
                if(index == 0)
                {
                    result = result + "{'" + metaData[index].getString(MetaData::ATTR_NAME).c_str() + "':'" + rs->getString(index+1).c_str() + "',";
                }
                else if(index == size-1)
                {
                    result = result + "'" + metaData[index].getString(MetaData::ATTR_NAME).c_str() + "':'" + rs->getString(index+1).c_str() + "'},";
                }
                else
                {
                    result = result + "'" + metaData[index].getString(MetaData::ATTR_NAME).c_str() + "':'" + rs->getString(index+1).c_str() + "',";
                }
            }
            if(size == 1)
            {
                result = result + "{'" + metaData[index].getString(MetaData::ATTR_NAME).c_str() + "':'" + rs->getString(index+1).c_str() + "'},";
            }
        }
    }
    result = result + "]";
    stmt->closeResultSet(rs);
    con->terminateStatement(stmt);
    env->terminateConnection(con);
    char *select_result = new char[result.length()+1];
    strcpy(select_result, result.c_str());
    return select_result;
}


/*
* delete a pointer
* @param: pointer
* @return: void
* */
void free_pointer(char *pointer)
{
    delete pointer;
}


/*
* create Oracle C++ Call Interface environment
* */
void create_environment()
{
    env = Environment::createEnvironment("al32utf8", "UTF8");
    cout << "create Oracle C++ Call Interface environment" <<endl;
}


/*
* close the Oracle C++ Call Interface environment
* */
void close_environment()
{
    Environment::terminateEnvironment(env);
    cout << "close the Oracle C++ Call Interface environment" << endl;
}


extern "C"
{
    bool execute_query(char *sql[], int length)
    {
        return execute(sql, length);
    }
    char *select_query(char *sql)
    {
        return select_sql(sql);
    }
    void open_oracle()
    {
        create_environment();
    }
    void close_oracle()
    {
        close_environment();
    }
    void delete_pointer(char *p)
    {
        free_pointer(p);
    }
}


int main() {
    /*
    * 此处代码
    * */
    create_environment();
    char a[] = "select * from manager_info";
    char *aa;
    aa = select_sql(a);
    cout << aa << endl;
//    Connection* con = env->createConnection(dbSetting.user, dbSetting.password, dbSetting.server);
//    char aaa[] = "insert into CONTENT_INFO VALUES ('6943f23e-926d-11e4-8feb-60d8194fce1f', 'test_once', 'hello world,it is just a test!你好世界，这只是一个测试！')";
//    execute_sql(con, aaa);
    close_environment();
    return 0;
}