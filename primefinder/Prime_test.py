def prime_test(n):
    #n: number need to test.
    #Return: Ture means n is prime, False means not.
    if n < 2:
        return False
    #use some small primes for preliminary quick check
    example_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29,31, 37]
    if n in example_primes:
        return True
    if any(n%p==0 for p in example_primes):
        return False
    #use Miller_Robin theorem to test prime
    d, s =n-1, 0
    while d%2==0:
        d //= 2
        s += 1
    #test_base, these 7 bases can check for 64-bit integers.
    # source: by Jim Sinclair, https://miller-rabin.appspot.com/
    test_base = [2, 325, 9375, 28178, 450775, 9780504, 1795265022]
    for a in test_base:
        if n % a==0:
            continue
        x = pow(a, d, n)
        if x == n-1 or x== 1:
            continue
        for _ in range(s-1):
            x = pow(x, 2, n)
            if x == n-1:
                break
        else:
            return False
    return True

# preprocessing of file (separate as some chunks, the best size of each chunk need to test).
def chunk_offset(file_path,chunk_line):
    offset = []
    with open(file_path,'rb') as f:
        line_count=0
        start_offset=0
        while True:
            line=f.readline()
            if not line:
                if line_count>0:
                    offset.append((start_offset,line_count))
                break
            line_count+=1
            if line_count==chunk_line:
                offset.append((start_offset,line_count))
                start_offset=f.tell()
                line_count=0
    return offset

#need to determine
def worker():
def coordinator():

#optimize,eg for the chunk_line size
import time
if __name__ == '__main__':
    start_time = time.time()
    '''
    '''
    end_time = time.time()
    print('Total time cost:', end_time-start_time)


