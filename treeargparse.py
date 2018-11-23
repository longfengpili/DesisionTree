import argparse
import datetime
parser = argparse.ArgumentParser(description='manual to this script')

today = datetime.date.today()
today = today.strftime('%Y-%m-%d') 

parser.add_argument('-sd','--start_date', type=str,help='input date',dest='start_date',default=today)
parser.add_argument('-ed','--end_date', type=str,help='input date',dest='end_date',default=today)
args = parser.parse_args()

input_start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d').date()
input_end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').date()


# print(input_date)
# print(type(input_date))