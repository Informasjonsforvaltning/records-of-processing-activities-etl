.PHONY: test
all: extract transform load

extract:
	echo "Extracting records...."
	python3 ./extract.py -o ./tmp/

transform:
	echo "Transforming records...."
	python3 ./transform.py -o ./tmp/

load:
	echo "Loading records...."
	python3 ./load.py -o ./tmp/

clean:
	rm ./tmp/*.json
