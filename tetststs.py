import traceback
import sys
# declaring and assigning array
A = [1, 2, 3, 4]

# exception handling
try:
    value = A[5]

except:
    # printing stack trace
    # traceback.print_exc()
    traceback.print_exception(*sys.exc_info())
# out of try-except
# this statement is to show
# that program continues normally
# after an exception is handled
print("end of program")