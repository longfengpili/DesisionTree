import argparse
parser = argparse.ArgumentParser(description='manual to this script')

parser.add_argument('-v', type=str, default = None)
# parser.add_argument('--batch-size', type=int, default=32)
args = parser.parse_args()
v = args.v
# batch_size = args.batch_size


print(v)