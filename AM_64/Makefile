am_main1:
	@echo " Compile am_main1 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/am_main1.c ./src/AM.c -lbf -o ./build/am_main1
	find -name EMP-AGE -print0 | xargs -0 -I file mv file ./last_EMP-AGE.db
	find -name EMP-NAME -print0 | xargs -0 -I file mv file ./last_EMP-NAME.db
	find -name EMP-DNAME -print0 | xargs -0 -I file mv file ./last_EMP-DNAME.db
	find -name EMP-FAULT -print0 | xargs -0 -I file mv file ./last_EMP-FAULT.db
	find -name EMP-SAL -print0 | xargs -0 -I file mv file ./last_EMP-SAL.db

am_main2:
	@echo " Compile am_main2 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/am_main2.c ./src/AM.c -lbf -o ./build/am_main2

am_main3:
	@echo " Compile am_main3 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/am_main3.c ./src/AM.c -lbf -o ./build/am_main3

bf_main1:
	@echo " Compile bf_main1 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main1.c -lbf -o ./build/bf_main1 -O2

bf_main2:
	@echo " Compile bf_main2 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main2.c -lbf -o ./build/bf_main2 -O2

bf_main3:
	@echo " Compile bf_main3 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main3.c -lbf -o ./build/bf_main3 -O2

