'''
DSCI 553 | Foundations and Applications of Data Mining
Homework 5
Matheus Schmitz
'''

from blackbox import BlackBox
import sys
import binascii
import random
import time


if __name__ == "__main__":
	random.seed(553)

	#start_time = time.time()

	# Read user inputs
	input_filename = sys.argv[1]
	stream_size = int(sys.argv[2])	
	num_of_asks = int(sys.argv[3])
	output_filename = sys.argv[4]

	# Sample sized is fixed at 100
	sample_size = 100

	# Global variable tracking the sequence number of the incoming users
	sequence_number = 0

	# List to store current items in the reservoir
	reservoir = []

	# Before beginning to iterate, write the column headers
	with open(output_filename, "w") as f_out:
		f_out.write("seqnum,0_id,20_id,40_id,60_id,80_id")

	# Blackbox
	BB = BlackBox()

	# Iterate over the asks
	for ask_iteration in range(num_of_asks):
		stream_users = BB.ask(input_filename, stream_size)
		
		# Go over all users for this stream
		for user in stream_users:

			# Update the sequence number for the current user
			sequence_number += 1

			# For all long as the reservoir has less samples that then cap, just keep adding users
			if len(reservoir) < sample_size:
				reservoir.append(user)

			# Once the reservoir fills, start sampling who gets in and who gets out
			else:
				# Sample if the next user should get in the reservoir
				if random.random() < (float(sample_size)/float(sequence_number)):
					
					# If the new user was chosen go get in, sample the index of the user to be swapped for the new user
					swap_idx =  random.randint(0, sample_size-1)
					
					# Then make the swap
					reservoir[swap_idx] = user

			# Every time you receive 100 users, you sohuld print the current stage of your reservoir to a CSV file
			if sequence_number % 100 == 0:

				# Then append the results to the output file
				with open(output_filename, "a") as f_out:
					f_out.write("\n" + str(sequence_number) + "," + reservoir[0].strip() + "," + reservoir[20].strip() + "," + reservoir[40].strip() + "," + reservoir[60].strip() + "," + reservoir[80].strip())
	
	# Measure the total time taken and report it
	#time_elapsed = time.time() - start_time
	#print('Duration: {}'.format(time_elapsed))