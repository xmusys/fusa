#include <cstdlib>

void flush_cache()
{
	const size_t size =
100 * 1024 * 1024; // 10MB ( CPU )
	volatile char *dummy = (volatile char *)malloc(size);
	for (size_t i = 0; i < size;
i += 64) { // ( 64 )
		dummy[i] = i % 256;
	}
	free((void *)dummy);
}

int main()
{
	flush_cache();
}