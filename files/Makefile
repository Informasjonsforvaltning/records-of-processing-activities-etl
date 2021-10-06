.PHONY: test
all: extract transform load

extract:
	echo "Extracting records & organizations...."
	python3 ./extract.py -o ./tmp/

transform:
	echo "Transforming records & organizations(-> representatives)...."
	python3 ./transform.py -o ./tmp/

load:
	echo "Loading records & representatives...."
	python3 ./load.py -o ./tmp/

clean:
	rm ./tmp/*.json
