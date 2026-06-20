#include <fstream>
#include <string>
#include <iostream>

class YCSBLoader {
    public:
	YCSBLoader()
	{
	}

	void load(std::string &file_path, char *load_methods,
		  uint64_t *load_keys, uint64_t load_num)
	{
		std::ifstream ifs;
		uint64_t key;
		ifs.open(file_path);
		char temp[100];
		if (!ifs) {
			free(load_methods);
			free(load_keys);
			std::cout << "open error in " << file_path << std::endl;
			exit(1);
		} else {
			for (int i = 0; i < load_num; i++) {
				ifs >> load_methods[i];
				ifs >> load_keys[i];

				ifs.getline(temp, 32);
			}
			ifs.close();
		}
	}
};