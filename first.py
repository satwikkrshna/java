#1st question answer

fashion = []

with open("winter_fashion.txt", "r") as f:
    next(f)
    for line in f:
        data = line.strip().split(",")
        brand = data[0]
        category = data[1]
        material = data[2]
        price = float(data[3])
        score = float(data[4])

        if material == "Wool":
            d = 5
        elif material == "Cotton":
            d = 7
        elif material == "Cashmere":
            d = 6
        elif material == "Leather":
            d = 2
        elif material == "Polyester":
            d = 10
        else:
            d = 0

        offer = round(price - (d * price / 100), 2)

        if score > 9:
            trend = "Emerging"
        elif score > 7:
            trend = "Trending"
        elif score > 5:
            trend = "Classic"
        else:
            trend = "Outdated"

        fashion.append([brand, category, material, price, score, offer, trend])

print("Brand,Categroy,Material,Price,Popularity_Score,OfferPrice,Trend")
for item in fashion:
    print(",".join([str(i) for i in item]))
