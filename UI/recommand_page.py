import sys
from PyQt5.QtWidgets import *
from datetime import datetime
from UI import recommand_page_ui


class newRecommandClass(QDialog):
    def __init__(self, mainEdit):
        super().__init__()

        # Recommand page 실행 시 UI 초기화
        self.recui = recommand_page_ui.Ui_MainWindow()
        self.recui.setupUi(self)

        # Main page의 Edittext 객체 담기
        self.mainEdit = mainEdit

        # 변수 초기화
        self.id = 0
        self.titles = []
        self.genres = []
        self.total_titles = []
        self.total_genres = []

    # 뒤로가기 버튼 누르면 창 닫음
    def goMain(self):
        self.mainEdit.clear()
        self.close()

    # Main page에서 입력한 아이디로 recommand page 상단의 id 설정
    def setId(self, id):
        self.id = id
        self.recui.label_21.setText(str(self.id))

    # 추천된 커스텀 결과를 테이블에 적용 (title, genre)
    def setResult(self, title_list, genre_list):
        self.titles = title_list
        self.genres = genre_list

        self.recui.label_24.setText(self.titles[0])
        self.recui.label_27.setText(self.titles[1])
        self.recui.label_29.setText(self.titles[2])
        self.recui.label_28.setText(self.titles[3])
        self.recui.label_30.setText(self.titles[4])

        self.recui.label_31.setText(self.genres[0])
        self.recui.label_32.setText(self.genres[1])
        self.recui.label_34.setText(self.genres[2])
        self.recui.label_33.setText(self.genres[3])
        self.recui.label_35.setText(self.genres[4])

    # 추천된 전체 결과를 테이블에 적용 (title, genre)
    def setResult2(self, title_list, genre_list):
        self.total_titles = title_list
        self.total_genres = genre_list

        self.recui.label_48.setText(self.total_titles[0])
        self.recui.label_46.setText(self.total_titles[1])
        self.recui.label_50.setText(self.total_titles[2])
        self.recui.label_51.setText(self.total_titles[3])
        self.recui.label_49.setText(self.total_titles[4])

        self.recui.label_52.setText(self.total_genres[0])
        self.recui.label_45.setText(self.total_genres[1])
        self.recui.label_42.setText(self.total_genres[2])
        self.recui.label_47.setText(self.total_genres[3])
        self.recui.label_43.setText(self.total_genres[4])