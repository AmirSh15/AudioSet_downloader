import csv


class Data:
    def __init__(
        self, csv_path, label_path, allowed_classes=None, exclude_classes=None
    ):
        self.csv_path = csv_path
        self.label_path = label_path
        self.allowed_classes = allowed_classes
        self.exclude_classes = exclude_classes

        self.index = []
        self.url = []
        self.slink = "https://www.youtube.com/watch?v="
        self.start = []
        self.end = []
        self.lables = []
        self.classes_name = []
        self.classes_code = []

        self.process()
        self.label_process()
        self.convert_labels()
        self.subset_selection()

        self.download_status = [False for _ in range(len(self.index))]

    def process(self):
        """
        This function read the content of data csv file and store their information.
        """
        csv_content = read_csv(self.csv_path)
        # Start form 3rd column because the priors are title
        for idx, row in enumerate(csv_content[3:]):
            self.index.append(idx)
            self.url.append(self.slink + row[0])
            self.start.append(float(row[1]))
            self.end.append(float(row[2]))
            self.lables.extend([row[3:]])

    def label_process(self):
        """
        This function read lable and corresponding codes from csv file.
        """
        labels = read_csv(self.label_path)
        self.classes_name = [label[2] for label in labels[1:]]
        self.classes_code = [label[1] for label in labels[1:]]

    def convert_labels(self):
        """
        This function converts codes to the actual class names.
        """
        for i, row in enumerate(self.lables):
            for j, label in enumerate(row):
                idx = self.classes_code.index(label.replace('"', "").replace(" ", ""))
                self.lables[i][j] = self.classes_name[idx]

    def subset_selection(self):
        """
        This function collect the subset of the database based on the choice of allowed_classes or
        exclude_classes.
        """
        selected_idx = []
        if self.allowed_classes != None:
            for idx, label in enumerate(self.lables):
                if self.allowed_classes in label:
                    selected_idx.append(idx)
            self.index = [self.index[idx] for idx in selected_idx]
            self.start = [self.start[idx] for idx in selected_idx]
            self.end = [self.end[idx] for idx in selected_idx]
            self.url = [self.url[idx] for idx in selected_idx]
            self.lables = [self.lables[idx] for idx in selected_idx]
        elif self.exclude_classes != None:
            for idx, label in enumerate(self.lables):
                if self.exclude_classes not in label:
                    selected_idx.append(idx)
            self.index = [self.index[idx] for idx in selected_idx]
            self.start = [self.start[idx] for idx in selected_idx]
            self.end = [self.end[idx] for idx in selected_idx]
            self.url = [self.url[idx] for idx in selected_idx]
            self.lables = [self.lables[idx] for idx in selected_idx]


def read_csv(path):
    file = open(path)
    content = list(csv.reader(file))
    file.close()
    return content
