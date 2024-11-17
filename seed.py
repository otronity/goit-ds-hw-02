import faker
import sqlite3
from random import randint, choice

NUMBER_USERS = 10
NUMBER_STATUS = 3
NUMBER_TASKS = 15


def create_db():
# читаємо файл зі скриптом для створення БД
    with open('create.sql', 'r') as f:
        sql = f.read()

# створюємо з'єднання з БД (якщо файлу з БД немає, він буде створений)
    with sqlite3.connect('user_tasks.db') as con:
        cur = con.cursor()
# виконуємо скрипт із файлу, який створить таблиці в БД
        cur.executescript(sql)


def generate_data(number_users, number_status, number_tasks) -> tuple():
    for_users = []
    for_status = [('new',), ('in progress',), ('completed',)]
    for_tasks = []

    fake_data = faker.Faker()

    for _ in range(number_users):
        name = fake_data.unique.name()
        email = fake_data.unique.email()
        for_users.append((name, email))

    for _ in range(number_tasks):
        title = fake_data.sentence(nb_words=6)
        description = fake_data.text()
        status_id = randint(1, number_status)
        user_id = randint(1, number_users)
        for_tasks.append((title, description, status_id, user_id))

    return for_users, for_status, for_tasks


def insert_data_to_db(users, status, tasks)-> None:
    with sqlite3.connect('user_tasks.db') as con:
        cur = con.cursor()
        sql_to_users = """INSERT INTO users(fullname, email)
                               VALUES (?, ?)"""
        cur.executemany(sql_to_users, users)
        sql_to_status = """INSERT INTO status(name)
                               VALUES (?)"""
        cur.executemany(sql_to_status, status)
        sql_to_tasks = """INSERT INTO tasks(title, description, status_id, user_id)
                               VALUES (?, ?, ?, ?)"""
        cur.executemany(sql_to_tasks, tasks)
        con.commit()


def select_query(sql: str) -> list:
    with sqlite3.connect('user_tasks.db') as con:
        cur = con.cursor()
        cur.execute(sql)
        return cur.fetchall()


def execute_query(sql: str):   
    try:
        with sqlite3.connect('user_tasks.db') as con:
            cur = con.cursor()
            # включаем внешние ключи для каскадного удаления
            con.execute("PRAGMA foreign_keys = ON;") 
            cur.execute(sql)
            con.commit()
            return "Запрос выполнен успешно"
    except sqlite3.IntegrityError as e:
        # Обрабатываем ошибку уникальности и выводим сообщение
        print(f"Ошибка при выполнении запроса: {e}")
        return "Ошибка уникальности"
    except Exception as e:
        # Обрабатываем любые другие ошибки
        print(f"Произошла ошибка: {e}")
        return "Произошла ошибка"
    

if __name__ == "__main__":
    create_db()
    users, status, tasks = generate_data(NUMBER_USERS, NUMBER_STATUS, NUMBER_TASKS)
    insert_data_to_db(users, status, tasks)

    # Отримати всі завдання певного користувача
    sql = """SELECT * FROM tasks where user_id = 1;"""
    print(select_query(sql))

    # Вибрати завдання за певним статусом
    sql = """SELECT * FROM tasks where status_id = 1;"""
    print(select_query(sql))

    # Оновити статус конкретного завдання
    sql = """update tasks set status_id = 1 where id = 6;"""
    print(execute_query(sql))

    # Отримати список користувачів, які не мають жодного завдання
    sql = """SELECT * FROM users t1
    left join tasks t2 on t2.user_id = t1.id
    where t2.user_id is null;"""
    print(select_query(sql))

    # Додати нове завдання для конкретного користувача
    sql = """insert into tasks(title, description, status_id, user_id)
      values('test_title', 'test_description', 1, 1);"""
    print(execute_query(sql))

    # Отримати всі завдання, які ще не завершено
    sql = """SELECT * FROM tasks where status_id != 3;"""
    print(select_query(sql))

    # Видалити конкретне завдання
    sql = """delete from tasks where id = 5;"""
    print(execute_query(sql))

    # Знайти користувачів з певною електронною поштою
    sql = """SELECT * FROM users where email = 'otronity@gmail.com';"""
    print(select_query(sql))

    # Оновити ім'я користувача
    sql = """update users set fullname = 'Olha Troino' where id = 5;"""
    print(execute_query(sql))

    # Отримати кількість завдань для кожного статусу
    sql = """select t1.id, t1.name, count(t2.id)
    from status t1
    left join tasks t2 on t2.status_id = t1.id
    group by t1.id, t1.name;"""
    print(select_query(sql))

    # Отримати завдання, які призначені користувачам з певною доменною частиною електронної пошти
    sql = """SELECT t2.* 
    FROM users t1
    join tasks t2 on t2.user_id = t1.id 
    where email like '%@example.org';"""
    print(select_query(sql))

    # Отримати список завдань, що не мають опису
    sql = """SELECT * FROM tasks where description is null;"""
    print(select_query(sql))

    # Вибрати користувачів та їхні завдання, які є у статусі 'in progress'
    sql = """SELECT t1.fullname, t2.*, t3.name 
    FROM users t1
    join tasks t2 on t2.user_id = t1.id
    join status t3 on t3.id = t2.status_id 
                    and t3.name = 'in progress';"""
    print(select_query(sql))

    # Отримати користувачів та кількість їхніх завдань
    sql = """select t1.id, t1.fullname, count(t2.id)
    from users t1
    left join tasks t2 on t2.user_id = t1.id 
    group by t1.id, t1.fullname;"""
    print(select_query(sql))