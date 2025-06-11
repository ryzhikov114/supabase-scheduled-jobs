import os
from supabase import create_client

# Получаем данные из переменных окружения
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

# Создаем клиент Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def main():
    try:
        # Пример: добавляем запись в таблицу 'tasks'
        new_task = {
            "name": "Автоматическая задача",
            "created_at": "now()",
            "status": "completed"
        }
        
        # Вставляем данные в таблицу
        response = supabase.table('tasks').insert(new_task).execute()
        
        # Проверяем результат
        if response.data:
            print("✅ Данные успешно добавлены в Supabase!")
            print(response.data)
        else:
            print("❌ Ошибка при добавлении данных")
            
    except Exception as e:
        print(f"❌ Произошла ошибка: {str(e)}")

if __name__ == "__main__":
    main()