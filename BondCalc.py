

class BondCalc:
    '''
    ## BondCalc 
    ### 用於執行與債券相關的各種計算。這些方法包括計算遠期利率、債券現值、即期利率、零息債券價格以及到期殖利率（YTM）。

    ### 方法：
    - forward_rate: 計算遠期利率。
    - price: 計算債券現值。
    - current_y: 計算平價債券的到期收益率。
    - interest_rate: 使用二分法計算即期利率。
    - zero_price: 計算零息債券價格。
    - ytm: 使用二分法計算債券的到期殖利率(YTM)。
    - horizon_ytm: 計算投資期限報酬率（到期殖利率）。

    ### 例子：
    bc = Bond_Calc()
    
    #### 計算遠期利率
    forward_rate = bc.forward_rate(r=0.05, r_n=5, x=0.04, x_n=3)

    #### 計算債券現值
    price = bc.price(rs=[0.03, 0.04, 0.05], y=0.04, n=1, p=100)

    #### 計算平價債券的到期收益率
    current_yield = bc.current_y(rs=[0.03, 0.04, 0.05])

    #### 使用二分法計算即期利率
    spot_rate = bc.interest_rate(rs=[0.03, 0.04, 0.05], y=0.04, n=1)

    #### 計算零息債券價格
    zero_coupon_price = bc.zero_price(r=0.05, year=3, n=1, p=100)

    #### 使用二分法計算債券的到期殖利率(YTM)
    ytm = bc.ytm(r=0.05, year=3, n=1, p_buy=95, p=100)

    #### 計算投資期限報酬率（到期殖利率）
    horizon_ytm = bc.horizon_ytm(rs=[0.03, 0.04, 0.05], horizon=3, y=0.04)
    '''

    
    def forward_rate(self, r: float = 0.0, r_n: int = 0, x: float = 0.0, x_n: int = 0, print_y=True) -> float:
        '''
        ### 計算遠期利率
        - r: float, 總年數利率
        - r_n: int, 總年數
        - x: float, 起算年利率
        - x_n: int, 起算年數
        '''
        y = (((1 + r) ** r_n) / ((1 + x) ** x_n)) ** (1 / (r_n - x_n)) - 1
        
        if print_y:
            print(f' {(r_n-x_n)} 年後的 {x_n} 年期遠期利率 : {round(y, 6)}')
        return y


    def price(self, rs: list[float], y: float, n:int=1, p: float = 100, print_p: bool = True) -> float:
        """ 
        ### 使用計算債券的現值
        - rs: List[float], 各期利率列表
        - y: float, 票息利率
        - p: float, 面值
        - print_p: bool, 是否打印現值
        """
        rs = [x/n for x in rs]
        maturity = len(rs)  # 年數
        price = 0
        # 計算每年的利息現值
        for t in range(1, maturity + 1):
            price += p * y / n / (1 + rs[t - 1])**t
        # 計算面值的現值
        price += p / (1 + rs[-1])**maturity
        
        if print_p == True:
            print(f' {(len(rs))} 年 {n} 期債券現值 : {round(price, 2)}')
        
        return price


    def current_y(self, rs: list[float], n:int=1, p:float=100) -> float:
        """
        ### 計算平價債券的到期收益率
        - rs: List[float], 各期利率列表
        - p: float, 面值
        """
        # 定義需要求解的函數
        def ytm_func(ytm):
            return self.price(rs, ytm, n, p, print_p=False) - p

        # 二分法迭代
        left = 0
        right = 1
        tolerance = 1e-6  # 設置容忍度
        while right - left > tolerance:
            mid = (left + right) / 2
            if ytm_func(mid) * ytm_func(left) > 0:
                left = mid
            else:
                right = mid

        y = (left + right) / 2
        print(f' {len(rs)} 年 {n} 期利率 : {round(y, 6)}')
        return y


    def interest_rate(self, rs: list[float], y: float, n:int=1) -> float:
        """
        ### 使用二分法計算即期利率
        - rs: List[float], 各期利率列表
        - y: float, YTM
        """
        # 目標搜索的預期現值
        target_present_value = 100
        
        # 定義目標函數
        def objective_function(r_n):
            rs_with_r_n = rs + [r_n]  # 將猜測值作為最後一年的貼現率加入到列表中
            present_value = self.price(rs_with_r_n, y, n, print_p=False)
            difference = present_value - target_present_value
            return difference

        # 設置二分法搜索範圍
        left = 0   
        right = 1 

        # 設定精度
        tolerance = 1e-6
        d = right - left # 定義迭代條件
        
        # 使用二分法搜索 r_n
        while d > tolerance:
            mid = (left + right) / 2
            difference = objective_function(mid)
            if difference < 0:
                right = mid
            else:
                left = mid
                
            d = right - left

        r_n = (left + right) / 2
        print(f' {(len(rs)+1)} 年 {n} 期即期利率 : {round(r_n, 6)}')
        return r_n


    def zero_price(self, r: float, year: int, n:int=1, p:float=100) -> float:   
        '''
        ### 計算零息債券價格
        - r: float, 利率
        - year (int): 債券期限（年）
        - n (int): 每年付息測次數
        - p: float, 面值
        '''
        p = p /( (1+r/n)**year)
        print(f' {year} 年 {n} 期零息債券價格為 : {round(p, 2)}')
        return p


    def ytm(self, r: float, year: int, n: int=1, p_buy: float=100, p: float=100) -> float:
        """
        ### 使用二分法計算債券的到期殖利率（YTM）。

        Args:
        - p_buy (float): 購買價格
        - p (float): 面值
        - year (int): 債券期限（年）
        - n (int): 每年付息測次數
        - r (float): 票面利率

        Returns:
        - float: 到期殖利率
        """
        tolerance = 1e-6  # 設置容忍度
        left = 0.0  # 初始下界
        right = 1.0  # 初始上界

        # 使用二分法搜索 YTM
        while right - left > tolerance:
            mid = [(left + right) / 2] * year * n
            calculated_price = self.price(mid, r, n , p, print_p=False)
            if calculated_price < p_buy:
                right = mid[0]
            else:
                left = mid[0]
        
        y = (left + right) / 2
        print(f' {year} 年 {n} 期利率 : {round(y, 6)}')

        return y


    def horizon_ytm(self, rs: list[float], horizon: int, y: float, p: float=100, p_buy: float=100, rs_to_fs=True) -> float:
        """
        ### 計算投資期限報酬率（到期殖利率）

        Args:
        - rs (list[float]): 各期利率列表(也可以是遠期利率)
        - horizon (int): 投資的總期數
        - y (float): 到期收益率
        - p (float, optional): 到期時的面值
        - p_buy (float, optional): 購買價格
        - rs_to_fs (bool, optional): 是否將利率列表轉換為遠期利率

        Returns:
        - float: 到期殖利率
        """
        if rs_to_fs:
            # 各年度的1年遠期利率
            f_rate = []
            for i in range(1,horizon+1):
                f = self.forward_rate(rs[i], i+1, rs[i-1], i, print_y=False)
                f_rate.append(f)
        else:
            f_rate = rs

        c_all = 0 
        for i in range(0, horizon):
            
            r = 1    # 初始化複利利率
            for f in [1+f for f in f_rate[i:-1]]:  
                r *= f
            c = r * p * y   # 債息＋債息再投資
            c_all += c

        p_sell = p * (1+ y) / (1+f_rate[-1]) #  投資期限結束時之債券價值
        r = ((p_sell+c_all) / p_buy) ** (1/horizon) - 1   # 投資期限報酬率
        print(f'持有 {len(f_rate)} 年投資期限利率 : {round(r, 6)}')
        
        return r









if __name__=="__main__":

    bond_calc = BondCalc()
    # 零息利率
    r_n_bisection = BondCalc.zero_price(0.0667, 6, 2)

    # 遠期利率
    y = bond_calc.forward_rate(0.044, 3, 0.0415, 2)

    # 到期價格
    rs = [0.08, 0.0875] 
    y = 0.06  # 每期的票息
    p = bond_calc.price(rs, y)

    # 到期利率
    rs = [0.015, 0.0175, 0.01875] 
    y = bond_calc.current_y(rs)

    # 即期利率
    rs = [0.015, 0.0175] 
    y = 0.016
    r_n_bisection = bond_calc.interest_rate(rs, y)

    # 到期收益率
    y = bond_calc.ytm(0.08, 7, 1, 101300, 100000)

    # 投資期限報酬率
    rs = [0.04, 0.03, 0.025, 0.02]
    y = bond_calc.horizon_ytm(rs, 3, 0.03, 100000, 101400)





