#include <stdlib.h>
#include <stdio.h>

#define N 81920
#define DT double

int main(int argc, char **argv) {
  int i, j;
  DT* arr = (DT*)malloc(sizeof(DT)*N);
  
  for (i = 0; i < 10; ++i) {
    for (j = 0; j < (N-1); ++j) {
      arr[j] = arr[j+1]+(DT)(1.0);
    }
  }
  
  free(arr);
  printf("Finished cachetest \n");
  return 0;
}
