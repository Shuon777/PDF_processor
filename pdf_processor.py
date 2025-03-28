import requests
import pymysql
import logging
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from pymysql import MySQLError


logging.basicConfig(
    filename='pdf_processor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


load_dotenv()


PDFCO_API_KEY = os.getenv("PDFCO_API_KEY")
API_BASE_URL = "https://api.pdf.co/v1"
UPLOAD_ENDPOINT = f"{API_BASE_URL}/file/upload"
CONVERT_ENDPOINT = f"{API_BASE_URL}/pdf/convert/to/jpg"


EXTERNAL_API_URL = os.getenv("EXTERNAL_API_URL", "https://httpbin.org/post")
EXTERNAL_API_KEY = os.getenv("EXTERNAL_API_KEY")


DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME"),
    'port': int(os.getenv("DB_PORT")),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def init_database():
    """Инициализация структуры БД"""
    tables = {
        'processed_files': """
        CREATE TABLE IF NOT EXISTS processed_files (
            id INT AUTO_INCREMENT PRIMARY KEY,
            original_filename VARCHAR(255) NOT NULL,
            processed_at DATETIME NOT NULL,
            pages_count INT NOT NULL,
            file_size BIGINT NOT NULL,
            status ENUM('processing', 'completed', 'failed') DEFAULT 'processing',
            result_path TEXT,
            external_api_response TEXT,
            INDEX idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        'processed_pages': """
        CREATE TABLE IF NOT EXISTS processed_pages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_id INT NOT NULL,
            page_number INT NOT NULL,
            file_path TEXT NOT NULL,
            FOREIGN KEY (file_id) REFERENCES processed_files(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    }
    
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        if not conn:
            return False
            
        with conn.cursor() as cursor:
            for table_name, sql in tables.items():
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    cursor.execute(sql)
                    logging.info(f"Таблица {table_name} создана")
        
        conn.commit()
        return True
    except MySQLError as e:
        logging.error(f"Ошибка инициализации БД: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
    
def upload_file(file_path):
    """Загрузка файла на PDF.co"""
    try:
        with open(file_path, 'rb') as f:
            response = requests.post(
                UPLOAD_ENDPOINT,
                files={'file': (os.path.basename(file_path), f)},
                headers={'x-api-key': PDFCO_API_KEY},
                timeout=30
            )
            response.raise_for_status()
            

            result = response.json()
            if 'url' in result:
                return result['url']
            elif 'presignedUrl' in result:
                return result['presignedUrl']
            else:
                raise ValueError("Неверный формат ответа: URL не найден")
                
    except Exception as e:
        logging.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def convert_pdf_to_jpg(file_url):
    """Конвертация PDF в JPG"""
    try:
        payload = {
            "url": file_url,
            "async": False,
            "outputformat": "jpg",
            "pages": "0-",  
            "inline": True
        }
        
        response = requests.post(
            CONVERT_ENDPOINT,
            json=payload,
            headers={
                'x-api-key': PDFCO_API_KEY,
                'Content-Type': 'application/json'
            },
            timeout=60
        )
        response.raise_for_status()
        
        result = response.json()
        if 'urls' in result:
            return result['urls']  
        elif 'url' in result:
            return [result['url']]  
        else:
            raise ValueError("Не найдены URL для скачивания")
            
    except Exception as e:
        logging.error(f"Ошибка конвертации: {str(e)}")
        return None

def download_files(url_list, output_dir, base_name):
    """Скачивание JPG файлов"""
    downloaded_files = []
    total_size = 0
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        for i, url in enumerate(url_list):
            output_path = os.path.join(output_dir, f"{base_name}_page_{i+1}.jpg")
            
            response = requests.get(
                url,
                headers={'x-api-key': PDFCO_API_KEY},
                stream=True,
                timeout=60
            )
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(output_path)
            downloaded_files.append({
                'page_number': i+1,
                'file_path': output_path,
                'file_size': file_size
            })
            total_size += file_size
            
        return {
            'pages': downloaded_files,
            'total_size': total_size
        }
        
    except Exception as e:
        logging.error(f"Ошибка скачивания: {str(e)}")
        return None

def send_to_external_api(data):
    """Отправка данных на внешний API"""
    headers = {
        'Content-Type': 'application/json'
    }
    if EXTERNAL_API_KEY:
        headers['Authorization'] = f"Bearer {EXTERNAL_API_KEY}"
    
    try:
        response = requests.post(
            EXTERNAL_API_URL,
            json=data,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Ошибка отправки на API: {str(e)}")
        return None

def save_to_db(file_info, api_response=None):
    """Сохранение данных в MySQL"""
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:

            sql = """
            INSERT INTO processed_files 
            (original_filename, processed_at, pages_count, file_size, result_path, external_api_response)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                file_info['filename'],
                datetime.now(),
                len(file_info['pages']),
                file_info['total_size'],
                file_info['output_dir'],
                str(api_response) if api_response else None
            ))
            file_id = cursor.lastrowid
            

            for page in file_info['pages']:
                cursor.execute(
                    "INSERT INTO processed_pages (file_id, page_number, file_path) VALUES (%s, %s, %s)",
                    (file_id, page['page_number'], page['file_path'])
                )
        
        conn.commit()
        return True
    except MySQLError as e:
        logging.error(f"Ошибка БД: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def process_pdf(file_path):
    """Основной процесс обработки"""
    if not os.path.exists(file_path):
        logging.error(f"Файл не найден: {file_path}")
        return False
    
    try:

        file_url = upload_file(file_path)
        if not file_url:
            return False
        

        jpg_urls = convert_pdf_to_jpg(file_url)
        if not jpg_urls:
            return False
        

        base_name = os.path.splitext(os.path.basename(file_path))[0]
        output_dir = os.path.join('output', base_name)
        
        download_result = download_files(jpg_urls, output_dir, base_name)
        if not download_result:
            return False
        

        file_info = {
            'filename': os.path.basename(file_path),
            'output_dir': output_dir,
            'pages': download_result['pages'],
            'total_size': download_result['total_size']
        }
        

        api_response = send_to_external_api({
            'original_file': file_info['filename'],
            'converted_files': [p['file_path'] for p in file_info['pages']],
            'timestamp': datetime.now().isoformat()
        })
        

        if not save_to_db(file_info, api_response):
            return False
            
        return True
        
    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")
        return False

if __name__ == "__main__":
    print("PDF to JPG Converter")
    print("---------------------")

    init_database()
    
    input_file = input("Введите путь к PDF файлу: ").strip()
    
    start_time = time.time()
    success = process_pdf(input_file)
    elapsed = time.time() - start_time
    
    if success:
        print(f"✅ Конвертация завершена за {elapsed:.2f} сек")
        print(f"Результаты в папке: output/{os.path.splitext(os.path.basename(input_file))[0]}/")
    else:
        print("❌ Ошибка обработки. Смотрите логи в pdf_processor.log")