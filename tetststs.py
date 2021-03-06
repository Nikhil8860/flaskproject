from bs4 import BeautifulSoup


def read_html(path):
    with open(path, 'r') as f:
        sum_age = 0
        soup = BeautifulSoup(f.read(), 'html.parser')
        t = [i.text for i in soup.find_all('td')]
        for i in range(2, len(t), 3):
            sum_age += int(t[i])
    return sum_age

# java


if __name__ == '__main__':
    file_path = "C:/Users/nikhils3/Desktop/table.html"
    print(read_html(file_path))
