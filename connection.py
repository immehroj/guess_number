import psycopg2

# Функция для подключения к базе данных
def connect_db():
    try:
        connection = psycopg2.connect(
            dbname="ConstructionDB",
            user="postgres",
            password="pg16",
            host="localhost",
            port="5432"
        )
        return connection
    except Exception as e:
        print("Ошибка при подключении к базе данных:", e)
        return None

# Добавление подрядчика в таблицу "contractors"
def add_contractor(contractor_id, name, address, phone, year_established):
    connection = None
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO Contractors (ContractorID, Name, Address, Phone, YearEstablished) VALUES (%s, %s, %s, %s, %s)", (contractor_id, name, address, phone, year_established))
            connection.commit()
            print("Подрядчик успешно добавлен")
    except Exception as e:
        print("Ошибка при добавлении подрядчика:", e)
    finally:
        if connection:
            connection.close()


# Добавление заказчика в таблицу "customers"
def add_customer(customer_id, name, address, phone):
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO Customers (CustomerID, Name, Address, Phone) VALUES (%s, %s, %s, %s)", (customer_id, name, address, phone))
            connection.commit()
            print("Заказчик успешно добавлен")
    except Exception as e:
        print("Ошибка при добавлении заказчика:", e)
    finally:
        cursor.close()
        connection.close()


# Добавление информации о строящемся здании в таблицу "BuildingProjects"
def add_building(contractor_id, customer_id, address, num_apartments, completion_year):
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO BuildingProjects (ContractorID, CustomerID, Address, NumApartments, CompletionYear) VALUES (%s, %s, %s, %s, %s)", (contractor_id, customer_id, address, num_apartments, completion_year))
            connection.commit()
            print("Информация о строящемся здании успешно добавлена")
    except Exception as e:
        print("Ошибка при добавлении информации о строящемся здании:", e)
    finally:
        cursor.close()
        connection.close()


def select_all_contractors(): # полная выборка
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM Contractors")
            contractors = cursor.fetchall()
            for contractor in contractors:
                print(contractor)
    except Exception as e:
        print("Ошибка при выполнении выборки из таблицы Contractors:", e)
    finally:
        cursor.close()
        connection.close()

def select_contractor_by_id(contractor_id): # выборка по id
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM Contractors WHERE ContractorID = %s", (contractor_id,))
            contractor = cursor.fetchone()
            if contractor:
                print(contractor)
            else:
                print("Подрядчик с указанным ID не найден.")
    except Exception as e:
        print("Ошибка при выполнении выборки из таблицы Contractors:", e)
    finally:
        cursor.close()
        connection.close()

def select_all_customers(): # полная выборка
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM Customers")
            customers = cursor.fetchall()
            for customer in customers:
                print(customer)
    except Exception as e:
        print("Ошибка при выполнении выборки из таблицы Customers:", e)
    finally:
        cursor.close()
        connection.close()

def select_customer_by_id(customer_id): # выборка по id
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM Customers WHERE CustomerID = %s", (customer_id,))
            customer = cursor.fetchone()
            if customer:
                print(customer)
            else:
                print("Заказчик с указанным ID не найден.")
    except Exception as e:
        print("Ошибка при выполнении выборки из таблицы Customers:", e)
    finally:
        cursor.close()
        connection.close()

def select_building_projects_by_contractor(contractor_id): # выборка с соединением таблиц
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT bp.*, c.Name AS CustomerName
                FROM BuildingProjects bp
                JOIN Customers c ON bp.CustomerID = c.CustomerID
                WHERE bp.ContractorID = %s
            """, (contractor_id,))
            projects = cursor.fetchall()
            for project in projects:
                print(project)
    except Exception as e:
        print("Ошибка при выполнении выборки из таблицы BuildingProjects:", e)
    finally:
        cursor.close()
        connection.close()

def update_contractor_address(contractor_id, new_address): # Обновление данных в таблице "Contractors"
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("UPDATE Contractors SET Address = %s WHERE ContractorID = %s", (new_address, contractor_id))
            connection.commit()
            print("Данные подрядчика успешно обновлены")
    except Exception as e:
        print("Ошибка при обновлении данных в таблице Contractors:", e)
    finally:
        cursor.close()
        connection.close()

def update_customer_phone(customer_id, new_phone): #
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("UPDATE Customers SET Phone = %s WHERE CustomerID = %s", (new_phone, customer_id))
            connection.commit()
            print("Данные заказчика успешно обновлены")
    except Exception as e:
        print("Ошибка при обновлении данных в таблице Customers:", e)
    finally:
        cursor.close()
        connection.close()

def update_customer_phone(customer_id, new_phone): # Обновление данных в таблице "Customers"
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("UPDATE Customers SET Phone = %s WHERE CustomerID = %s", (new_phone, customer_id))
            connection.commit()
            print("Данные заказчика успешно обновлены")
    except Exception as e:
        print("Ошибка при обновлении данных в таблице Customers:", e)
    finally:
        cursor.close()
        connection.close()

def update_project_details(contractor_id, num_apartments, completion_year):
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("""
                UPDATE BuildingProjects 
                SET NumApartments = %s, CompletionYear = %s 
                WHERE ContractorID = %s
            """, (num_apartments, completion_year, contractor_id))
            connection.commit()
            print("Данные проектов успешно обновлены")
    except Exception as e:
        print("Ошибка при обновлении данных в таблице BuildingProjects:", e)
    finally:
        cursor.close()
        connection.close()

def delete_contractor_by_id(contractor_id):
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()

            # Удаляем связанные проекты из таблицы BuildingProjects
            cursor.execute("DELETE FROM BuildingProjects WHERE ContractorID = %s", (contractor_id,))
            connection.commit()

            # Затем удаляем подрядчика из таблицы Contractors
            cursor.execute("DELETE FROM Contractors WHERE ContractorID = %s", (contractor_id,))
            connection.commit()

            print("Данные подрядчика и связанные проекты успешно удалены")
    except Exception as e:
        print("Ошибка при удалении данных из таблицы Contractors:", e)
    finally:
        cursor.close()
        connection.close()

def delete_all_contractors():
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM Contractors")
            connection.commit()
            print("Все данные о подрядчиках успешно удалены")
    except Exception as e:
        print("Ошибка при удалении всех данных из таблицы Contractors:", e)
    finally:
        cursor.close()
        connection.close()

def delete_customer_by_id(customer_id):
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM Customers WHERE CustomerID = %s", (customer_id,))
            connection.commit()
            print("Данные заказчика успешно удалены")
    except Exception as e:
        print("Ошибка при удалении данных из таблицы Customers:", e)
    finally:
        cursor.close()
        connection.close()

def delete_all_customers():
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM Customers")
            connection.commit()
            print("Все данные о заказчиках успешно удалены")
    except Exception as e:
        print("Ошибка при удалении всех данных из таблицы Customers:", e)
    finally:
        cursor.close()
        connection.close()


def delete_projects_by_completion_year(year):
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM BuildingProjects WHERE CompletionYear = %s", (year,))
            connection.commit()
            print("Данные проектов успешно удалены")
    except Exception as e:
        print("Ошибка при удалении данных из таблицы BuildingProjects:", e)
    finally:
        cursor.close()
        connection.close()

def delete_all_projects():
    try:
        connection = connect_db()
        if connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM BuildingProjects")
            connection.commit()
            print("Все данные о проектах успешно удалены")
    except Exception as e:
        print("Ошибка при удалении всех данных из таблицы BuildingProjects:", e)
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    delete_projects_by_completion_year(2015)  # Удалить проекты по году завершения