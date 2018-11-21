import argparse
import datetime
parser = argparse.ArgumentParser(description='manual to this script')

today = datetime.date.today()
today = today.strftime('%Y-%m-%d') 

parser.add_argument('-d','--date', type=str,help='input date',dest='date',default=today)
args = parser.parse_args()

input_date = args.date


# print(input_date)