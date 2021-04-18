# Create a DAG Operation Pipeline for String
from abc import ABC, abstractmethod


class Pipe(ABC):
    @abstractmethod
    def operate(self, input_string):
        pass


class Add_Suffix(Pipe):
    def __init__(self, pipe: Pipe, char_):
        self._pipe = pipe
        self._char = char_

    def operate(self, input_string):
        return self._pipe.operate(input_string) + self._char


class Add_Prefix(Pipe):
    def __init__(self, pipe: Pipe, char_):
        self._pipe = pipe
        self._char = char_

    def operate(self, input_string):
        return self._char + self._pipe.operate(input_string)


class Tap(Pipe):
    def __init__(self):
        pass

    def operate(self, input_string):
        return input_string


class Lowercase(Pipe):
    def __init__(self, pipe):
        self._pipe = pipe

    def operate(self, input_string):
        return self._pipe.operate(input_string).lower()

## ---- DO NOT TOUCH BELOW ---- ##

end_point = Tap()

end_point = Add_Prefix(end_point, 'd')
end_point = Add_Prefix(end_point, 'S')
end_point = Lowercase(end_point)
end_point = Add_Suffix(end_point, '*')
end_point = Add_Suffix(end_point, 'K')
assert end_point.operate('Innovaccer') == 'sdinnovaccer*K'
