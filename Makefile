all: ./build/main

test: ./build/test

./build/test: ./build/test_main.o ./build/data.o ./build/time_utils.o ./build/ht.o ./build/analytics.o
	mpicc -Wall -g -o ./build/test ./build/test_main.o ./build/data.o ./build/time_utils.o ./build/ht.o ./build/analytics.o -lm

./build/main: ./build/main.o ./build/data.o ./build/time_utils.o ./build/ht.o ./build/analytics.o
	mpicc -Wall -g -o ./build/main ./build/main.o ./build/data.o ./build/time_utils.o ./build/ht.o ./build/analytics.o -lm

./build/main.o: main.c
	mpicc -Wall -c -o ./build/main.o main.c -g

./build/test_main.o: test_main.c
	mpicc -Wall -c -o ./build/test_main.o test_main.c -g

./build/data.o: ./utils/data.c ./utils/data.h
	mpicc -Wall -c -o ./build/data.o ./utils/data.c -g

./build/time_utils.o: ./utils/time_utils.c ./utils/time_utils.h
	mpicc -Wall -c -o ./build/time_utils.o ./utils/time_utils.c -g

./build/ht.o: ./utils/ht.c ./utils/ht.h
	mpicc -Wall -c -o ./build/ht.o ./utils/ht.c -g

./build/analytics.o: ./utils/analytics.c ./utils/analytics.h
	mpicc -Wall -c -o ./build/analytics.o ./utils/analytics.c -g

clean:
	rm -r build && mkdir ./build