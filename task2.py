'''
DSCI 553 | Foundations and Applications of Data Mining
Homework 5
Matheus Schmitz
'''

from blackbox import BlackBox
import sys
import binascii
import random
import math
import time


# Hyperparameters to create hash functions
n_groups = 11
n_rows = 10
n_hash = int(n_groups * n_rows)
m = n_hash * n_groups

# Generate values for the hash functions
hash_params = [[random.randint(1, 100), random.randint(1, 100)] for _ in range(n_hash)]


def myhashs(user):

	# Encode user to int
	user_int = int(binascii.hexlify(user.encode('utf8')),16)

	# Generate hash values
	result = []
	for f in hash_params:
	    result.append((f[0] * user_int + f[1]) % m)

	return result


def count_trailing_zeroes(binary_as_string):
	return len(str(binary_as_string)) - len(str(binary_as_string).rstrip('0'))


if __name__ == "__main__":

	#start_time = time.time()

	# Read user inputs
	input_filename = sys.argv[1]
	stream_size = int(sys.argv[2])	
	num_of_asks = int(sys.argv[3])
	output_filename = sys.argv[4]


	# Before beginning to iterate, write the column headers
	with open(output_filename, "w") as f_out:
		f_out.write("Time,Ground Truth,Estimation")

	# Blackbox
	BB = BlackBox()

	# Iterate over the asks
	for ask_iteration in range(num_of_asks):
		stream_users = BB.ask(input_filename, stream_size)

		# Set to store all users seen in this iteration
		seen_users_truth = set()

		# Lists to store the hash binary representations generated
		hash_bin = []

		# Go over all users for this stream
		for user in stream_users:

			# Add the user to the set of seen users
			seen_users_truth.add(user)

			# Hash the user into values
			hashed_idxs = myhashs(user)

			# Store all binary values for the current user (one value per hash function)
			iter_hash_bin = []

			# For the current user, get the hashed index and its binary representation
			for curr_idx in hashed_idxs:
				user_bin = bin(curr_idx)[2:]
				iter_hash_bin.append(user_bin)

			# Add the hashed values from the current iteration (current user) to the list of all hashes
			hash_bin.append(iter_hash_bin)

		# For each of the generated binary encoding of hash values, calculate the distance based on the number of trailing zeroes
		estimated_size_per_hash = []

		# Iterate through all hash functions
		for curr_hash in range(n_hash):
			curr_hash_max_zeroes = 0

			# Then, for a given hash function, go over the binary encodings generated for all users
			for curr_user in range(len(hash_bin)):

				# Count the number of trailing zeroes for the current user with the current hash
				curr_user_max_zeroes = count_trailing_zeroes(hash_bin[curr_user][curr_hash])
				
				# If it is longer than the previous max values for the current hash, then update the max value
				if curr_user_max_zeroes > curr_hash_max_zeroes:
					curr_hash_max_zeroes = curr_user_max_zeroes

			# Once the largest number of trailing zeroes for a given has hash function has been found, calculate the estimated size and append it to the list of estimates
			estimated_size_per_hash.append(math.pow(2, curr_hash_max_zeroes))

		# Slice the estimated sizes in "n_groups", then for each group calculate the group average
		group_avgs = []
		for group_idx in range(0, n_groups):
			group_sum = 0.0

			# Loop over the rows in the group
			for curr_row in range(0, n_rows):

				# Get the row index to be fetched from "estimated_size_per_hash" which has all estimates
				row_idx = group_idx*n_rows + curr_row

				# Fetch the estimate for the current row and add it to the sum of estimates for the current group
				group_sum += estimated_size_per_hash[row_idx]

			# Calcualte the average for the current group and append it to the list of all group averages
			group_avg = group_sum / n_rows
			group_avgs.append(group_avg)

		# Get the median value from the group averages by sorting them and taking the middle number
		group_avgs = sorted(group_avgs)
		distinct_users_prediction = int(group_avgs[int(n_groups/2)])
		
		# Then append the results to the output file
		with open(output_filename, "a") as f_out:
			f_out.write("\n" + str(ask_iteration) + "," + str(len(seen_users_truth)) + "," + str(distinct_users_prediction))

	# Measure the total time taken and report it
	#time_elapsed = time.time() - start_time
	#print('Duration: {}'.format(time_elapsed))