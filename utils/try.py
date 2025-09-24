
def numTrees( n: int) -> int:
    if n <= 1: return 1
    return sum(numTrees(i-1) * numTrees(n-i) for i in range(1, n+1))

print(numTrees(13))
print(numTrees(9))


