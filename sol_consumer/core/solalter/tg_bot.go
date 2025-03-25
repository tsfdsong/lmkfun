package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ethereum/go-ethereum/common"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/config"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/alikafka"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/model"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
)

type TgButtonInfo struct {
	Text string `json:"text"`
	URL  string `json:"url"`
}

type TgMarkup struct {
	InlineKeyboard [][]TgButtonInfo `json:"inline_keyboard"`
}

type TgMessage struct {
	Webhook     string   `json:"webhook"`
	ChatID      []string `json:"chat_id"`
	Msg         string   `json:"msg"`
	ReplyMarkup TgMarkup `json:"reply_markup"`
	CreateTime  int      `json:"create_time"`
	KeepTime    int      `json:"keep_time"`
}

func makeMarkup(listid, chain, token string) TgMarkup {
	dexschain := chain
	if strings.ToLower(chain) == "eth" {
		dexschain = "ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dexschain = "solana"
	} else if strings.ToLower(chain) == "base" {
		dexschain = "base"
	} else if strings.ToLower(chain) == "bsc" {
		dexschain = "bsc"
	}

	res := make([][]TgButtonInfo, 0)

	firstlevel := make([]TgButtonInfo, 0)
	if listid != "" {
		txhis := TgButtonInfo{
			Text: "🕰TxHistory",
			URL:  fmt.Sprintf("https://lmk.fun/detail/%s", listid),
		}

		firstlevel = append(firstlevel, txhis)
	}

	if dexschain != "" && token != "" {
		chart := TgButtonInfo{
			Text: "📊Chart",
			URL:  fmt.Sprintf("https://dexscreener.com/%s/%s", dexschain, token),
		}

		// if dexschain == "solana" {
		// 	chart = TgButtonInfo{
		// 		Text: "📊Chart",
		// 		URL:  fmt.Sprintf("https://lmk.fun/dex/%s/%s", dexschain, token),
		// 	}
		// }

		firstlevel = append(firstlevel, chart)
	}

	if len(firstlevel) > 0 {
		res = append(res, firstlevel)
	}

	if strings.ToLower(chain) == "solana" {
		secondlevel := make([]TgButtonInfo, 0)
		trade := TgButtonInfo{
			Text: "⚡️Trade Now",
			URL:  fmt.Sprintf("https://t.me/lmkfotfunsol1bot?start=m_buy_t_%s", token),
		}

		secondlevel = append(secondlevel, trade)

		if len(secondlevel) > 0 {
			res = append(res, secondlevel)
		}
	}

	return TgMarkup{
		InlineKeyboard: res,
	}
}

func GetBotInfo(listid string) (*model.TgBotInfo, error) {
	var resAddr model.TgBotInfo
	query := fmt.Sprintf(`select * from multichain_view_ads.view_ads_lmk_customer_local_bots WHERE list_id = '%s'`, listid)
	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resAddr)
	if err != nil {
		return nil, err
	}

	return &resAddr, nil
}

func SetTgBotInfoCache(listid string) error {
	data, err := GetBotInfo(listid)
	if err != nil {
		return err
	}

	if data != nil {
		key := fmt.Sprintf("bot:%s", data.ListID)

		bytes, err := json.Marshal(&data)
		if err != nil {
			return err
		}
		err = redis.Set(context.Background(), key, string(bytes), time.Hour)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetTgBotInfoCache(listId string) (*model.TgBotInfo, error) {
	key := fmt.Sprintf("bot:%s", listId)
	data, err := redis.Get(context.Background(), key)
	if err == redis.Nil {
		err = SetTgBotInfoCache(listId)
		if err != nil {
			return nil, err
		}

		data, err = redis.Get(context.Background(), key)
	}
	if err != nil {
		return nil, err
	}

	var res model.TgBotInfo
	err = json.Unmarshal([]byte(data), &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

func EscapeSpecialCharacters(text string) string {

	escaped := map[string]string{
		"_": "\\_",
		"*": "\\*",
		"[": "\\[",
		"]": "\\]",
		"(": "\\(",
		")": "\\)",
		"~": "\\~",
		"`": "\\`",
		">": "\\>",
		"#": "\\#",
		"+": "\\+",
		"-": "\\-",
		"=": "\\=",
		"|": "\\|",
		"{": "\\{",
		"}": "\\}",
		".": "\\.",
		"!": "\\!",
	}

	for old, new := range escaped {
		text = strings.ReplaceAll(text, old, new)
	}

	// specialCharacters := []string{"_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"}

	// for _, char := range specialCharacters {
	// 	text = strings.ReplaceAll(text, char, "\\"+char)
	// }
	return text
}

func formatString(input string) string {
	if len(input) <= 10 {
		return input
	}

	prefix := input[:6]
	suffix := input[len(input)-4:]

	return fmt.Sprintf("%s...%s", prefix, suffix)
}

func ConstructCreateBotMessage(chain, fromLabel, fromAccount, toTokenSymbol, listid, totoken string, ispublic bool) string {
	dispchain := chain
	if strings.ToLower(chain) == "eth" {
		dispchain = "Ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dispchain = "Solana"
	} else if strings.ToLower(chain) == "base" {
		dispchain = "Base"
	}

	fromaddr := fromAccount
	if !ispublic {
		fromaddr = "PrivateAddress"
	}

	ll := fmt.Sprintf("*Address Alert\n🦜%s*\n\n", EscapeSpecialCharacters("#"+fromaddr))
	if fromLabel != "" {
		ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://solscan.io/account/%s", totoken)))
	}
	tt := "🛠*Created:* " + EscapeSpecialCharacters(fmt.Sprintf("$%s(%s) on Pump.fun\n\n", toTokenSymbol, totoken)) + fmt.Sprintf("*Chain:* %s\n", dispchain)
	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return ll + tt + tail
}

func ConstructBuyBotMessage(chain, fromLabel, fromAccount, fromAmount, fromSymbol, fromValue, toAmount, toTokenSymbol, price, txHash, fromToken, listid, totoken string, ispublic bool, tradeLabel, tomc string) string {
	dexschain := chain
	dispchain := chain

	if strings.ToLower(chain) == "eth" {
		dexschain = "ethereum"
		dispchain = "Ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dexschain = "solana"
		dispchain = "Solana"
	} else if strings.ToLower(chain) == "base" {
		dexschain = "base"
		dispchain = "Base"
	} else if strings.ToLower(chain) == "bsc" {
		dexschain = "bsc"
		dispchain = "Binance"
		fromAccount = common.HexToAddress(fromAccount).String()
	}

	fromaddr := fromAccount
	if !ispublic {
		fromaddr = "PrivateAddress"
	}

	ll := fmt.Sprintf("*Address Alert\n🦜%s*\n\n", EscapeSpecialCharacters("#"+fromaddr))
	if fromLabel != "" {
		if ispublic {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/%s/%s?maker=%s", dexschain, totoken, fromAccount)))
			// if dexschain == "solana" {
			// 	ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://lmk.fun/dex/%s/%s", dexschain, totoken)))
			// }
		} else {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("*%s*\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"))
		}
	}
	tl := "🔥*Bought:* "
	if tradeLabel == LabelFirstBuy {
		tl = "💎*First Buy:*"
	}

	tt := tl + EscapeSpecialCharacters(fmt.Sprintf("%s $%s($%s) for %s $%s\n", toAmount, toTokenSymbol, fromValue, fromAmount, fromSymbol)) + fmt.Sprintf("*Price:* %s\n*Market Cap:* %s\n\n", EscapeSpecialCharacters("$"+price), EscapeSpecialCharacters("$"+convertMcap(tomc))) + fmt.Sprintf("*Chain:* %s\n", dispchain)

	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return ll + tt + tail
}

func ConstructSoldBotMessage(chain, fromLabel, fromAccount, fromAmount, fromSymbol, toValue, toAmount, toTokenSymbol, price, txHash, toToken, listid, fromtoken string, ispublic bool, tradeLabel, frommc string) string {
	dexschain := chain
	dispchain := chain

	if strings.ToLower(chain) == "eth" {
		dexschain = "ethereum"
		dispchain = "Ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dexschain = "solana"
		dispchain = "Solana"
	} else if strings.ToLower(chain) == "base" {
		dexschain = "base"
		dispchain = "Base"
	} else if strings.ToLower(chain) == "bsc" {
		dexschain = "bsc"
		dispchain = "Binance"
		fromAccount = common.HexToAddress(fromAccount).String()
	}

	fromaddr := fromAccount
	if !ispublic {
		fromaddr = "PrivateAddress"
	}

	ll := fmt.Sprintf("*Address Alert\n🦜%s*\n\n", EscapeSpecialCharacters("#"+fromaddr))
	if fromLabel != "" {
		if ispublic {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/%s/%s?maker=%s", dexschain, fromtoken, fromAccount)))
			// if dexschain == "solana" {
			// 	ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://lmk.fun/dex/%s/%s", dexschain, fromtoken)))
			// }
		} else {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("*%s*\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"))
		}
	}
	tl := "🗑*Sold:* "
	if tradeLabel == LabelSellAll {
		tl = "🗑*Sell All:*"
	}
	tt := tl + EscapeSpecialCharacters(fmt.Sprintf("%s $%s($%s) for %s $%s\n", fromAmount, fromSymbol, toValue, toAmount, toTokenSymbol)) + fmt.Sprintf("*Price:* %s\n*Market Cap:* %s\n\n", EscapeSpecialCharacters("$"+price), EscapeSpecialCharacters("$"+convertMcap(frommc))) + fmt.Sprintf("*Chain:* %s\n", dispchain)

	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return ll + tt + tail
}

func ConstructSendBotMessage(chain, fromLabel, fromAccount, toAccount, token, amount, symbol, value, price, txHash, listid string, ispublic bool) string {
	dexschain := chain
	dispchain := chain
	if strings.ToLower(chain) == "eth" {
		dexschain = "ethereum"
		dispchain = "Ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dexschain = "solana"
		dispchain = "Solana"
	} else if strings.ToLower(chain) == "base" {
		dexschain = "base"
		dispchain = "Base"
	}

	fromaddr := fromAccount
	if !ispublic {
		fromaddr = "PrivateAddress"
	}

	ll := fmt.Sprintf("*Address Alert\n🦜%s*\n\n", EscapeSpecialCharacters("#"+fromaddr))
	if fromLabel != "" {
		if ispublic {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/%s/%s?maker=%s", dexschain, token, fromAccount)))
			// if dexschain == "solana" {
			// 	ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://lmk.fun/dex/%s/%s", dexschain, token)))
			// }
		} else {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("*%s*\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"))
		}
	}
	tt := "🪙*Sent:* " + EscapeSpecialCharacters(fmt.Sprintf("%s $%s($%s) to %s\n\n", amount, symbol, value, toAccount)) + fmt.Sprintf("*Chain:* %s\n", dispchain)
	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return ll + tt + tail
}

func ConstructSendBotMessageNoTo(chain, fromLabel, fromAccount, toAccount, token, amount, symbol, value, price, txHash, listid string, wallcounts int, ispublic bool) string {
	dexschain := chain
	dispchain := chain
	if strings.ToLower(chain) == "eth" {
		dexschain = "ethereum"
		dispchain = "Ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dexschain = "solana"
		dispchain = "Solana"
	} else if strings.ToLower(chain) == "base" {
		dexschain = "base"
		dispchain = "Base"
	}

	fromaddr := fromAccount
	if !ispublic {
		fromaddr = "PrivateAddress"
	}

	ll := fmt.Sprintf("*Address Alert\n🦜%s*\n\n", EscapeSpecialCharacters("#"+fromaddr))
	if fromLabel != "" {
		if ispublic {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/%s/%s?maker=%s", dexschain, token, fromAccount)))
			// if dexschain == "solana" {
			// 	ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://lmk.fun/dex/%s/%s", dexschain, token)))
			// }
		} else {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("*%s*\n\n", EscapeSpecialCharacters(fromLabel+" ("+fromaddr+")"))
		}
	}
	tt := "🪙*Sent:* " + EscapeSpecialCharacters(fmt.Sprintf("%s $%s($%s) to %d wallets in a single transaction\n\n", amount, symbol, value, wallcounts)) + fmt.Sprintf("*Chain:* %s\n", dispchain)
	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return ll + tt + tail
}

func ConstructReceivedBotMessage(chain, fromAccount, toLabel, toAccount, token, amount, symbol, value, price, txHash, listid string, ispublic bool) string {
	dexschain := chain
	dispchain := chain

	if strings.ToLower(chain) == "eth" {
		dexschain = "ethereum"
		dispchain = "Ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dexschain = "solana"
		dispchain = "Solana"
	} else if strings.ToLower(chain) == "base" {
		dexschain = "base"
		dispchain = "Base"
	}

	toaddr := toAccount
	if !ispublic {
		toaddr = "PrivateAddress"
	}

	ll := fmt.Sprintf("*Address Alert\n🦜%s*\n\n", EscapeSpecialCharacters("#"+toaddr))
	if toLabel != "" {
		if ispublic {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(toLabel+" ("+toaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/%s/%s?maker=%s", dexschain, token, toAccount)))
			// if dexschain == "solana" {
			// 	ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(toLabel+" ("+toaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://lmk.fun/dex/%s/%s", dexschain, token)))
			// }
		} else {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("*%s*\n\n", EscapeSpecialCharacters(toLabel+" ("+toaddr+")"))
		}
	}
	tt := "🪙*Received:* " + EscapeSpecialCharacters(fmt.Sprintf("%s $%s($%s) from %s\n\n", amount, symbol, value, fromAccount)) + fmt.Sprintf("*Chain:* %s\n", dispchain)
	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return ll + tt + tail
}

func ConstructReceivedBotMessageNoFrom(chain, fromAccount, toLabel, toAccount, token, amount, symbol, value, price, txHash, listid string, wallcounts int, ispublic bool) string {
	dexschain := chain
	dispchain := chain

	if strings.ToLower(chain) == "eth" {
		dexschain = "ethereum"
		dispchain = "Ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dexschain = "solana"
		dispchain = "Solana"
	} else if strings.ToLower(chain) == "base" {
		dexschain = "base"
		dispchain = "Base"
	}

	toaddr := toAccount
	if !ispublic {
		toaddr = "PrivateAddress"
	}

	ll := fmt.Sprintf("*Address Alert\n🦜%s*\n\n", EscapeSpecialCharacters("#"+toaddr))
	if toLabel != "" {
		if ispublic {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(toLabel+" ("+toaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/%s/%s?maker=%s", dexschain, token, toAccount)))
			// if dexschain == "solana" {
			// 	ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(toLabel+" ("+toaddr+")"), EscapeSpecialCharacters(fmt.Sprintf("https://lmk.fun/dex/%s/%s", dexschain, token)))
			// }
		} else {
			ll = "*Address Alert*\n🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("*%s*\n\n", EscapeSpecialCharacters(toLabel+" ("+toaddr+")"))
		}
	}
	tt := "🪙*Received:* " + EscapeSpecialCharacters(fmt.Sprintf("%s $%s($%s) from %d wallets in a single transaction\n\n", amount, symbol, value, wallcounts)) + fmt.Sprintf("*Chain:* %s\n", dispchain)
	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return ll + tt + tail
}

func ConstructExchangeMessage(listid, exchangename, chain, tokenAddress, tokensymbol, title, annouurl string) string {
	if chain != "" && tokenAddress != "" {
		tokenaddr := tokenAddress

		if strings.ToLower(chain) == "solana" {
			tokenaddr = fmt.Sprintf("[%s](%s)\n", EscapeSpecialCharacters(tokenAddress), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/solana/%s", tokenAddress)))
		}

		dsm := ""
		if tokensymbol != "" {
			dsm = fmt.Sprintf("\n$%s", tokensymbol)
		}
		tt := "*Exchange Announcement\n" + EscapeSpecialCharacters(fmt.Sprintf("🦜#%s\n%s", exchangename, dsm)) + "*\n\n"
		tn := fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(title), EscapeSpecialCharacters(annouurl))
		if exchangename == "bybit" {
			tn = fmt.Sprintf("*%s*\n\n", EscapeSpecialCharacters(title))
		}
		ca := "*Chain:* " + EscapeSpecialCharacters(chain) + "\n*CA:* " + tokenaddr + "\n"
		tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

		return tt + tn + ca + tail
	}

	dsm := ""
	if tokensymbol != "" {
		dsm = fmt.Sprintf("\n$%s", EscapeSpecialCharacters(tokensymbol))
	}

	tt := fmt.Sprintf("*Exchange Announcement\n🦜%s\n%s*\n\n", EscapeSpecialCharacters("#"+exchangename), dsm)
	tn := fmt.Sprintf("[*%s*](%s)\n\n", EscapeSpecialCharacters(title), EscapeSpecialCharacters(annouurl))
	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return tt + tn + tail
}

func convertMcap(mcap string) string {
	value, _ := strconv.ParseFloat(mcap, 64)

	// 单位后缀和阈值
	const (
		kilo        = 1000.0
		mega        = 1000000.0
		bill        = 1000000000.0
		trillion    = 1000000000000.0
		quordillion = 1000000000000000.0
		quintillion = 1000000000000000000.0
		sextillion  = 1000000000000000000000.0
		septillion  = 1000000000000000000000.0
	)

	switch {
	case value >= septillion:
		return fmt.Sprintf("%.1fY", math.Floor(value/septillion*10)/10)
	case value >= sextillion:
		return fmt.Sprintf("%.1fZ", math.Floor(value/sextillion*10)/10)
	case value >= quintillion:
		return fmt.Sprintf("%.1fE", math.Floor(value/quintillion*10)/10)
	case value >= quordillion:
		return fmt.Sprintf("%.1fP", math.Floor(value/quordillion*10)/10)
	case value >= trillion:
		return fmt.Sprintf("%.1fT", math.Floor(value/trillion*10)/10)
	case value >= bill:
		return fmt.Sprintf("%.1fB", math.Floor(value/bill*10)/10)
	case value >= mega:
		return fmt.Sprintf("%.1fM", math.Floor(value/mega*10)/10)
	case value >= kilo:
		return fmt.Sprintf("%.1fK", math.Floor(value/kilo*10)/10)
	default:
		return fmt.Sprintf("%.1f", math.Floor(value*10)/10)
	}
}

func ConvertToPercentage(input string) string {

	value, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return "0%"
	}

	percentage := math.Floor(value*1000) / 10

	return fmt.Sprintf("%.1f%%", percentage)
}

func ConstructKOLMessage(listid, author, sentiment, chain, contractAddr, tokensymbol, mcap, tweeturl string, isverified bool) string {
	st := ""
	if strings.ToLower(sentiment) == "positive" {
		st = "Bullish"
	} else if strings.ToLower(sentiment) == "negative" {
		st = "Bearish"
	} else {
		st = "Bullish"
	}

	dispchain := chain
	if strings.ToLower(chain) == "eth" {
		dispchain = "Ethereum"
	} else if strings.ToLower(chain) == "solana" {
		dispchain = "Solana"
	} else if strings.ToLower(chain) == "base" {
		dispchain = "Base"
	} else if strings.ToLower(chain) == "bsc" {
		dispchain = "Binance"
	}

	if chain != "" || contractAddr != "" {
		al := "*KOL Mention\n*🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s) %s *%s*\n\n", EscapeSpecialCharacters(author), EscapeSpecialCharacters(tweeturl), EscapeSpecialCharacters("-"), EscapeSpecialCharacters(st))

		tip := "\n\n"
		if !isverified {
			tip = EscapeSpecialCharacters("(Derived from Ticker DYOR)\n\n")
		}
		tt := fmt.Sprintf("📞*$%s:* %s\n%s*Chain:* %s          *MCap:* $%s\n", EscapeSpecialCharacters(tokensymbol), EscapeSpecialCharacters(contractAddr), tip, EscapeSpecialCharacters(dispchain), EscapeSpecialCharacters((convertMcap(mcap))))
		ant := fmt.Sprintf("[*Tweet Link*](%s) \n", EscapeSpecialCharacters(tweeturl))
		tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

		return al + tt + ant + tail
	}

	al := "*KOL Mention\n*🦜" + EscapeSpecialCharacters("#") + fmt.Sprintf("[*%s*](%s) %s *%s*\n\n", EscapeSpecialCharacters(author), EscapeSpecialCharacters(tweeturl), EscapeSpecialCharacters("-"), EscapeSpecialCharacters(st))
	tt := fmt.Sprintf("📞*$%s*\n%s\n\n", EscapeSpecialCharacters(tokensymbol), EscapeSpecialCharacters("💡Only the token ticker is recognized. Please be aware of the risks."))
	ant := fmt.Sprintf("[*Tweet Link*](%s) \n", EscapeSpecialCharacters(tweeturl))
	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return al + tt + ant + tail
}

func SendKafkaBotMsg(in *TgMessage) error {
	data, err := json.Marshal(&in)
	if err != nil {
		return err
	}

	cfg := config.GetKafkaConfig()
	err = alikafka.GetKafkaProInst().Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &cfg.ProducerTopic, Partition: kafka.PartitionAny},
		Value:          []byte(data),
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	alikafka.GetKafkaProInst().Flush(1000)

	return nil
}

func ConstructCuratedMessage(listid, calltype, chainstr, contractAddr, tokensymbolstr, thesis, mcap, price, amount string) string {
	if chainstr == "" || contractAddr == "" {
		return ""
	}

	chain := chainstr
	tokensymbol := tokensymbolstr
	if chainstr == "eth" {
		chain = "eth"
		tokensymbol = "ETH"
	} else if chainstr == "solana" {
		chain = "solana"
		tokensymbol = "SOL"
	} else if chainstr == "base" {
		chain = "base"
		tokensymbol = "ETH"
	} else if chainstr == "bsc" {
		chain = "bsc"
		tokensymbol = "BNB"
	}

	al := "🦜*Curated Token Calls* " + EscapeSpecialCharacters("-") + " *DYOR*\n📞" + EscapeSpecialCharacters("#") + fmt.Sprintf("*%s*\n", EscapeSpecialCharacters(calltype))
	tt := fmt.Sprintf("*CA:* [%s](%s)\n\n", EscapeSpecialCharacters(contractAddr), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/%s/%s", chain, contractAddr)))
	// if chain == "solana" {
	// 	tt = fmt.Sprintf("*CA:* [%s](%s)\n\n", EscapeSpecialCharacters(contractAddr), EscapeSpecialCharacters(fmt.Sprintf("https://lmk.fun/dex/solana/%s", contractAddr)))
	// }

	pc := tokensymbol
	if calltype == "SellALL" || calltype == "DecreasePosition" {
		pc = "%"
	}
	ts := fmt.Sprintf("🧠*Thesis:* %s \n\n", EscapeSpecialCharacters(thesis)) + EscapeSpecialCharacters("#") + fmt.Sprintf("*SuggestedAmount:* %s %s\n\n", EscapeSpecialCharacters(amount), EscapeSpecialCharacters(pc))
	sa := fmt.Sprintf("*Symbol:* $%s          *Chain:* %s\n", EscapeSpecialCharacters(tokensymbol), EscapeSpecialCharacters(chain))
	ms := fmt.Sprintf("*MCap:* $%s", EscapeSpecialCharacters((convertMcap(mcap)))) + fmt.Sprintf("          *Price:* $%s \n", EscapeSpecialCharacters(price))

	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return al + tt + ts + sa + ms + tail
}

func ConstructFomoMessage(data *RawFomoCallsData) string {
	chain := data.Chain
	tokensymbol := data.TokenSymbol

	dispchain := ""
	if strings.ToLower(chain) == "solana" {
		dispchain = "Solana"
	}

	al := "🦜*Fomo Call*\n"

	mb := "Yes"
	if !data.MultyBuy {
		mb = "No"
	}

	wc := fmt.Sprintf("%d wallet", data.SellWalletCount)
	if data.SellWalletCount != 1 {
		wc = fmt.Sprintf("%d wallets", data.SellWalletCount)
	}

	dx := fmt.Sprintf("[*%s*](%s)\n", EscapeSpecialCharacters(tokensymbol), EscapeSpecialCharacters(fmt.Sprintf("https://dexscreener.com/%s/%s", chain, data.TokenAddress)))
	// if chain == "solana" {
	// 	dx = fmt.Sprintf("[*%s*](%s)\n", EscapeSpecialCharacters(tokensymbol), EscapeSpecialCharacters(fmt.Sprintf("https://lmk.fun/dex/solana/%s", data.TokenAddress)))
	// }
	tt := fmt.Sprintf("💹*%d* wallets have bought %s%s\n\n*Total bought:* %s SOL\n*Average Price:* %s\n*Average Mcap:* %s\n*Multiple Buys?* %s\n*Has Anyone sold?* %s\n\n",
		data.BuyWalletCount, dx, EscapeSpecialCharacters(fmt.Sprintf("(within %dmin)", data.SpacingTime)), EscapeSpecialCharacters(fmt.Sprintf("%f", data.BuySolAmount)), EscapeSpecialCharacters(fmt.Sprintf("$%f", data.AvgBuyPrice)), EscapeSpecialCharacters(fmt.Sprintf("$%s", convertMcap(fmt.Sprintf("%f", data.AvgBuyMc)))), mb, wc)

	ts := fmt.Sprintf("*Chain:* %s\n", EscapeSpecialCharacters(dispchain))

	tail := "*Notifier:* " + EscapeSpecialCharacters("lmk.fun")

	return al + tt + ts + tail
}
