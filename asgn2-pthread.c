#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include "util.h"

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
int notYetOccupiedStartIndex = 0;

typedef struct ranked_point {
    Point point;
    int *dimensionRanks;
    int ranksSum;
    bool isPrevailed;
} RankedPoint;

typedef struct pthread_input {
    RankedPoint *rankedPoints;
    int numberOfPoints;
    int numberOfDimensions;
    int batchSize;
} PthreadInput;

int dimension_compar(const void *v1, const void *v2, void *arg) {
    const int comparingDimension = *(int *) arg;
    const float fa = ((RankedPoint *) v1)->point.values[comparingDimension];
    const float fb = ((RankedPoint *) v2)->point.values[comparingDimension];
    return (fa > fb) - (fa < fb);
}

int ranks_sum_compar(const void *v1, const void *v2) {
    const float fa = ((RankedPoint *) v1)->ranksSum;
    const float fb = ((RankedPoint *) v2)->ranksSum;
    return (fa > fb) - (fa < fb);
}

void *computeIsPrevailed(void *input) {
    int i, j, k = 0;
    int numberOfPrevailedValues, numberOfPrevailingValues = 0;
    int *currentPointDimensionRanks;

    PthreadInput *pthreadInput = (PthreadInput *) input;

    RankedPoint *rankedPoints = pthreadInput->rankedPoints;
    int numberOfPoints = pthreadInput->numberOfPoints;
    int numberOfDimensions = pthreadInput->numberOfDimensions;
    int batchSize = pthreadInput->batchSize;

    int startIndex = 0;
    int endIndex = 0;

    RankedPoint currentPoint, comparingPoint;

    bool hasFourDimensions = numberOfDimensions > 3;
    bool hasFiveDimensions = numberOfDimensions > 4;
    bool hasMoreThanFiveDimensions = numberOfDimensions > 5;

    while (1) {
        if (notYetOccupiedStartIndex >= numberOfPoints) {
            break;
        }

        // Get start and end index
        pthread_mutex_lock(&mutex);

        startIndex = notYetOccupiedStartIndex;
        endIndex = startIndex + batchSize;

        // Set new index
        notYetOccupiedStartIndex = endIndex;

        pthread_mutex_unlock(&mutex);

        if (endIndex >= numberOfPoints) {
            endIndex = numberOfPoints;
        }

        // Compute
        for (i = startIndex; i != endIndex; i++) {
            currentPoint = rankedPoints[i];
            currentPointDimensionRanks = currentPoint.dimensionRanks;

            if (currentPoint.isPrevailed == true) {
                continue;
            }

            // Find if there is prevailing points
            for (j = 0; j != numberOfPoints; j++) {
                comparingPoint = rankedPoints[j];

                if (comparingPoint.isPrevailed == true) {
                    continue;
                }

                numberOfPrevailedValues = 0;
                numberOfPrevailingValues = 0;

                // Dimension - 0
                if (currentPointDimensionRanks[0] > comparingPoint.dimensionRanks[0]) {
                    numberOfPrevailedValues = numberOfPrevailedValues + 1;
                } else if (currentPointDimensionRanks[0] < comparingPoint.dimensionRanks[0]) {
                    numberOfPrevailingValues = numberOfPrevailingValues + 1;
                } else {
                    goto COMPARE_NEXT_POINT;
                }

                // Dimension - 1
                if (currentPointDimensionRanks[1] > comparingPoint.dimensionRanks[1]) {
                    if (numberOfPrevailingValues != 0) {
                        goto COMPARE_NEXT_POINT;
                    }

                    numberOfPrevailedValues = numberOfPrevailedValues + 1;
                } else if (currentPointDimensionRanks[1] < comparingPoint.dimensionRanks[1]) {
                    if (numberOfPrevailedValues != 0) {
                        goto COMPARE_NEXT_POINT;
                    }

                    numberOfPrevailingValues = numberOfPrevailingValues + 1;
                } else {
                    goto COMPARE_NEXT_POINT;
                }

                // Dimension - 2
                if (currentPointDimensionRanks[2] > comparingPoint.dimensionRanks[2]) {
                    if (numberOfPrevailingValues != 0) {
                        goto COMPARE_NEXT_POINT;
                    }

                    numberOfPrevailedValues = numberOfPrevailedValues + 1;
                } else if (currentPointDimensionRanks[2] < comparingPoint.dimensionRanks[2]) {
                    if (numberOfPrevailedValues != 0) {
                        goto COMPARE_NEXT_POINT;
                    }

                    numberOfPrevailingValues = numberOfPrevailingValues + 1;
                } else {
                    goto COMPARE_NEXT_POINT;
                }

                // Dimension - 3
                if (hasFourDimensions == true)
                    if (currentPointDimensionRanks[3] > comparingPoint.dimensionRanks[3]) {
                        if (numberOfPrevailingValues != 0) {
                            goto COMPARE_NEXT_POINT;
                        }

                        numberOfPrevailedValues = numberOfPrevailedValues + 1;
                    } else if (currentPointDimensionRanks[3] < comparingPoint.dimensionRanks[3]) {
                        if (numberOfPrevailedValues != 0) {
                            goto COMPARE_NEXT_POINT;
                        }

                        numberOfPrevailingValues = numberOfPrevailingValues + 1;
                    } else {
                        goto COMPARE_NEXT_POINT;
                    }

                // Dimension - 4
                if (hasFiveDimensions == true)
                    if (currentPointDimensionRanks[4] > comparingPoint.dimensionRanks[4]) {
                        if (numberOfPrevailingValues != 0) {
                            goto COMPARE_NEXT_POINT;
                        }

                        numberOfPrevailedValues = numberOfPrevailedValues + 1;
                    } else if (currentPointDimensionRanks[4] < comparingPoint.dimensionRanks[4]) {
                        if (numberOfPrevailedValues != 0) {
                            goto COMPARE_NEXT_POINT;
                        }

                        numberOfPrevailingValues = numberOfPrevailingValues + 1;
                    } else {
                        goto COMPARE_NEXT_POINT;
                    }

                if (hasMoreThanFiveDimensions == true)
                    for (k = 5; k < numberOfDimensions; k++) {
                        if (currentPointDimensionRanks[k] > comparingPoint.dimensionRanks[k]) {
                            if (numberOfPrevailingValues != 0) {
                                goto COMPARE_NEXT_POINT;
                            }

                            numberOfPrevailedValues = numberOfPrevailedValues + 1;
                        } else if (currentPointDimensionRanks[k] < comparingPoint.dimensionRanks[k]) {
                            if (numberOfPrevailedValues != 0) {
                                goto COMPARE_NEXT_POINT;
                            }

                            numberOfPrevailingValues = numberOfPrevailingValues + 1;
                        } else {
                            goto COMPARE_NEXT_POINT;
                        }
                    }

                // All current point values are bigger than comparing point values
                if (numberOfPrevailedValues == numberOfDimensions) {
                    rankedPoints[i].isPrevailed = true;
                    break;
                }
                // This is not necessary, but for improving performance
                if (numberOfPrevailingValues == numberOfDimensions) {
                    rankedPoints[j].isPrevailed = true;
                    break;
                }

                COMPARE_NEXT_POINT:;
            }
        }
    }

    return NULL;
}

int asgn2_pthread(Point *points, Point **pPermissiblePoints, int numberOfPoints, int numberOfDimensions, int numberOfThreads) {
    // points -- input data
    // pPermissiblePoints -- your computed answer
    // numberOfPoints -- numberOfPoints of points in dataset
    // numberOfDimensions -- the numberOfDimensions of the dataset
    // numberOfThreads -- the numberOfPoints of threads (cores) to use

    int i, j = 0;

    RankedPoint *rankedPoints = (RankedPoint *) malloc(numberOfPoints * sizeof(RankedPoint));
    int *allDimensionRanks = (int *) malloc(numberOfPoints * numberOfDimensions * sizeof(int));
    for (i = 0; i < numberOfPoints * numberOfDimensions; i++) {
        allDimensionRanks[i] = 0;
    }
    for (i = 0; i < numberOfPoints; i++) {
        rankedPoints[i].point = points[i];
        rankedPoints[i].dimensionRanks = &allDimensionRanks[i * numberOfDimensions];
        rankedPoints[i].isPrevailed = false;
        rankedPoints[i].ranksSum = 0;
    }

    // Get ranks on each dimension
    for (i = 0; i < numberOfDimensions; i++) {
        qsort_r(rankedPoints, numberOfPoints, sizeof(RankedPoint), dimension_compar, &i);

        int rank = 0;
        for (j = 0; j < numberOfPoints; j++) {
            rankedPoints[j].dimensionRanks[i] = rank;
            rankedPoints[j].ranksSum += rank;

            // if next value is same, ignore (rank + 1)
            if (j + 1 < numberOfPoints &&
                rankedPoints[j].point.values[i] != rankedPoints[j + 1].point.values[i]) {
                rank = rank + 1;
            }
        }
    }

    qsort(rankedPoints, numberOfPoints, sizeof(RankedPoint), ranks_sum_compar);

    // Initialize threads
    pthread_t *threads = (pthread_t *) malloc(numberOfThreads * sizeof(pthread_t));
    for (i = 0; i < numberOfThreads; i++) {
        PthreadInput *pthreadInput = malloc(sizeof(PthreadInput));
        pthreadInput->rankedPoints = rankedPoints;
        pthreadInput->numberOfPoints = numberOfPoints;
        pthreadInput->numberOfDimensions = numberOfDimensions;
        pthreadInput->batchSize = 128 + i * 4;

        pthread_create(&threads[i], NULL, computeIsPrevailed, pthreadInput);
    }

    for (i = 0; i < numberOfThreads; i++) {
        pthread_join(threads[i], NULL);
    }

    Point *permissiblePoints = (Point *) malloc(numberOfPoints * sizeof(Point));

    int numberOfRemainingPoints = 0;
    for (i = 0; i < numberOfPoints; i++) {
        if (rankedPoints[i].isPrevailed == false) {
            permissiblePoints[numberOfRemainingPoints] = rankedPoints[i].point;
            numberOfRemainingPoints++;
        }
    }

    *pPermissiblePoints = permissiblePoints;

    // Clean up

    free(rankedPoints);
    free(allDimensionRanks);
    free(threads);

    notYetOccupiedStartIndex = 0;

    // Clean up end

    return numberOfRemainingPoints;
}
