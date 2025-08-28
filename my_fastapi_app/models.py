import mysql.connector

# DB 연결 정보 (사용자 정보에 맞게 수정하세요!)
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "1234"
DB_NAME = "bangu"
DB_PORT = 3310

 
class DataManager:
    """데이터베이스와 상호작용하는 모델 클래스"""
    
    def get_db_connection(self):
        try:
            conn = mysql.connector.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            return conn
        except mysql.connector.Error as err:
            print(f"DB 연결 오류: {err}")
            return None

    def get_all_items(self):
        """데이터베이스에서 모든 항목을 가져오는 함수"""
        conn = self.get_db_connection()
        if not conn:
            return []
        
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM room2")
            items = cursor.fetchall()
            return items
        except mysql.connector.Error as err:
            print(f"쿼리 실행 오류: {err}")
            return []
        finally:
            cursor.close()
            conn.close()