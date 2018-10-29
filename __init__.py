import sys
sys.path.append('..')
import Tree_setting as tsetting

database = tsetting.database
user = tsetting.user
password = tsetting.password
host = tsetting.host
port = tsetting.port
app_name = tsetting.app_name 
puzzle_language = tsetting.puzzle_language
days = tsetting.days
random_state = int(tsetting.random_state)
mode_dir = tsetting.model_dir
test_size = int(tsetting.test_size)




if __name__ == '__main__':
    print(database)
    print(user)
    print(password)
    print(host)
    print(port)
    print(app_name)
    print(puzzle_language)
    print(days)