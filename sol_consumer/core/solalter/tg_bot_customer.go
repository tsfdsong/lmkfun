package solalter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/db"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/core/redis"
	"github.com/thescopedao/solana_dex_subscribe/sol_consumer/utils/logger"
)

type ChannelInfo struct {
	ChatID    string `json:"chatId"`
	GroupLink string `json:"groupLink"`
}

type TgBotCustomerInfo struct {
	ListID   string        `bun:"list_id"`
	ChatIDs  []ChannelInfo `bun:"channel_info"`
	BotToken string        `bun:"bot_token"`
}

func getChatIDs(in *TgBotCustomerInfo) []string {
	res := make([]string, 0)

	for _, v := range in.ChatIDs {
		res = append(res, v.ChatID)
	}

	return res
}

func GetUserBotInfo() ([]TgBotCustomerInfo, error) {
	var resAddr []TgBotCustomerInfo
	query := `select * from multichain_view_ads.view_ads_lmk_customer_bots;`

	err := db.GetDB().NewRaw(query).Scan(context.Background(), &resAddr)
	if err != nil {
		return nil, err
	}

	return resAddr, nil
}

func SetTgUserBotInfoCache(listid string) error {
	data, err := GetUserBotInfo()
	if err != nil {
		return err
	}

	for _, v := range data {
		if v.ListID != listid {
			continue
		}

		key := fmt.Sprintf("bot:user:%s", v.ListID)

		bytes, err := json.Marshal(&v)
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

func GetTgUserBotInfoCache(listId string) (*TgBotCustomerInfo, error) {
	key := fmt.Sprintf("bot:user:%s", listId)
	data, err := redis.Get(context.Background(), key)
	if err == redis.Nil {
		err = SetTgUserBotInfoCache(listId)
		if err != nil {
			return nil, err
		}

		data, err = redis.Get(context.Background(), key)
	}
	if err != nil {
		return nil, err
	}

	var res TgBotCustomerInfo
	err = json.Unmarshal([]byte(data), &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

func HandleTgBotMessage(listid, msg, chain, tokenAddress string, createtime int, isdisplayhistory bool) error {
	botinfo, err := GetTgBotInfoCache(listid)
	if err != nil {
		botinfo, err = GetTgBotInfoCache(listid)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ListID": listid, "TgMsg": msg, "ErrMsg": err}).Warn("HandleTgBotMessage GetTgBotInfoCache warn")
		}
	}

	markup := makeMarkup(listid, chain, tokenAddress)
	if !isdisplayhistory {
		markup = makeMarkup("", chain, tokenAddress)
	}

	if botinfo != nil && botinfo.ChatID != "" {
		hook := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", botinfo.BotToken)
		tgmsg := &TgMessage{
			Webhook:     hook,
			ChatID:      []string{botinfo.ChatID},
			Msg:         msg,
			ReplyMarkup: markup,
			CreateTime:  createtime,
			KeepTime:    600,
		}

		logger.Logrus.WithFields(logrus.Fields{"ListID": listid, "TgMsg": tgmsg}).Info("HandleTgBotMessage local bot info")

		err = SendKafkaBotMsg(tgmsg)
		if err != nil {
			err = SendKafkaBotMsg(tgmsg)
			if err != nil {
				logger.Logrus.WithFields(logrus.Fields{"ListID": listid, "Tg": tgmsg, "ErrMsg": err}).Error("HandleTgBotMessage SendKafkaBotMsg failed")
			}
		}
	} else {
		logger.Logrus.WithFields(logrus.Fields{"ListID": listid, "TgMsg": msg}).Info("HandleTgBotMessage local chatid empty")
	}

	//user customer bot tg
	userbotinfo, err := GetTgUserBotInfoCache(listid)
	if err != nil {
		userbotinfo, err = GetTgUserBotInfoCache(listid)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ListID": listid, "TgMsg": msg, "ErrMsg": err}).Warn("HandleTgBotMessage GetTgUserBotInfoCache failed")

			return nil
		}
	}

	chatids := getChatIDs(userbotinfo)
	if len(chatids) == 0 {
		logger.Logrus.WithFields(logrus.Fields{"ListID": listid, "TgMsg": msg}).Info("HandleTgBotMessage getChatIDs empty")

		return nil
	}

	userhook := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", userbotinfo.BotToken)
	usertgmsg := &TgMessage{
		Webhook:     userhook,
		ChatID:      chatids,
		Msg:         msg,
		ReplyMarkup: markup,
		CreateTime:  createtime,
		KeepTime:    600,
	}

	logger.Logrus.WithFields(logrus.Fields{"ListID": listid, "TgMsg": usertgmsg}).Info("HandleTgBotMessage user bot info")

	err = SendKafkaBotMsg(usertgmsg)
	if err != nil {
		err = SendKafkaBotMsg(usertgmsg)
		if err != nil {
			logger.Logrus.WithFields(logrus.Fields{"ListID": listid, "Tg": usertgmsg, "ErrMsg": err}).Error("HandleTgBotMessage SendKafkaBotMsg failed")

			return fmt.Errorf("send user bot, %v", err)
		}
	}

	return nil
}
