
import random

# Generate test1000.txt
f = open('data/test1000.txt', 'w')
[f.write(str(random.randint(2, 10**7)) + '\n') for _ in range(1000)]
f.close()

# Generate test10000.txt
f = open('data/test10000.txt', 'w')
[f.write(str(random.randint(2, 10**7)) + '\n') for _ in range(10000)]
f.close()

print("Generated test1000.txt and test10000.txt")