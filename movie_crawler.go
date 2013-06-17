package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Link struct {
	Url  string
	Rate float64
}

type Movie struct {
	Url         string
	Name        []string
	Year        uint64
	Category    []string
	Director    []string
	Charactor   []string
	District    []string
	ReleaseDate string
	Length      string
	Alias       []string
	Douban      Link
	Imdb        Link
}

type Page struct {
	url  string
	html string
}

var site string = "http://dianying.fm"
var detail_prefix string = "http://dianying.fm/movie"

var movie_folder = "movie/"

func file_get_contents(url string) string {
	defer func() { //捕捉异常
		if err := recover(); err != nil {
			fmt.Println("err:", err) //这里的err其实就是panic传入的内容
		}
	}()

	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("get url err: %v", err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	return string(body)
}

var tagPattern = regexp.MustCompile("\\<[\\S\\s]+?\\>")
var stylePattern = regexp.MustCompile("\\<style[\\S\\s]+?\\</style\\>")
var scriptPattern = regexp.MustCompile("\\<script[\\S\\s]+?\\</script\\>")
var newLinePattern = regexp.MustCompile("\\n+")
var symbolPattern = regexp.MustCompile("&{0,1}(amp|nbsp|quot);")
var commentPattern = regexp.MustCompile("<!--.+-->")

func cleanName(src string) string {
	//remove symbol
	return symbolPattern.ReplaceAllString(src, " ")
}

func cleanCommentTag(src string) string {
	src = newLinePattern.ReplaceAllString(src, "")
	src = commentPattern.ReplaceAllString(src, "")
	src = tagPattern.ReplaceAllString(src, "")
	return src
}

//data regex
var NamePattern = regexp.MustCompile(`x-m-title">([^<]+)<`)
var yearPattern = regexp.MustCompile(`x-m-title">[^<]+<span class="muted">\s*\((\d+)\)\s*</span>`)
var tBodyPattern = regexp.MustCompile(`\S+</table>`)
var ratePattern = regexp.MustCompile(`>([\d\.]+)</span>`)

func analyze(page_chan chan Page, fetched_link_chan chan string, record_chan chan Movie) {
	page_chan_closed := false
	for {
		if page_chan_closed {
			close(fetched_link_chan)
			close(record_chan)
			fmt.Println("analyze returned")
			return
		}
		//fmt.Println("Waiting for a new Page ...")
		select {
		case page, page_chan_ok := <-page_chan:
			if !page_chan_ok {
				page_chan_closed = true
				fmt.Println(" ... Page channel is closed!")
			} else {

				fmt.Println("analzing new Page", page.url)
				fetched_links := fetchLinks(page)
				movie := fetchRecord(page)
				fmt.Println(" finish analyze", page.url)
				if (len(movie.Name)) > 0 {
					record_chan <- movie
					fmt.Println("send record", movie.Name[0])
				} else {
					fmt.Println("no record", page.url)
				}

				for _, fetched_link := range fetched_links {
					if int64(len(linkmap)) < max_count {
						hash_str := gethash(fetched_link)
						_, ok := linkmap[hash_str]
						if !ok { // not exist
							linkmap[hash_str] = false
							fetched_link_chan <- fetched_link
							fetchedlinksCnt++
							// fmt.Println("send fetched link", fetched_link)
						}
					}
				}
			}
		default:
			fmt.Println("analyze have a rest")
			time.Sleep(200 * time.Millisecond)
		}
	}
}

//TODO
// /movie/...   /category/key_{..}?p=\d+
var linkPattern = regexp.MustCompile(`href="([^"]+)"`)

func fetchLinks(page Page) []string {
	var links []string
	for _, v := range linkPattern.FindAllStringSubmatch(page.html, 1000) {
		if strings.HasPrefix(v[1], "/category") || strings.HasPrefix(v[1], "/movie") { //detail and category
			//if strings.HasPrefix(v[1], "/movie") { //detail page only
			if !strings.Contains(v[1], "class_tv") && !strings.Contains(v[1], "sort_") && !strings.Contains(v[1], "region_") && !strings.Contains(v[1], "genre_") && !strings.Contains(v[1], "year_") { // only class_movie
				links = append(links, v[1]) // real link site+v[1]
			}
		}
	}
	return links
}

func fetchRecord(page Page) Movie {
	var movie Movie
	name_strs := NamePattern.FindStringSubmatch(page.html)
	if debug {
		log(name_strs)
	}
	if len(name_strs) < 1 {
		return movie
	}

	movie.Name = append(movie.Name, strings.TrimSpace(cleanName(name_strs[1])))
	//fmt.Println(movie.Name)
	movie.Url = page.url

	year_strs := yearPattern.FindStringSubmatch(page.html)
	//fmt.Println(year_strs)
	if len(year_strs) > 1 {
		movie.Year, _ = strconv.ParseUint(year_strs[1], 10, 16)
	}

	table := page.html[strings.Index(page.html, "<table"):strings.Index(page.html, "</table>")]

	for {
		start, end := strings.Index(table, "<tr"), strings.Index(table, "</tr>")
		if start == -1 || end == -1 {
			break
		}
		tr_str := table[start:end]
		// fmt.Println(tr_str, "\n")
		split := strings.LastIndex(tr_str, "<td")
		key := cleanCommentTag(tr_str[:split])
		value := tr_str[split:]
		if key == "评分" {
			for _, a := range strings.Split(value, "</a>") {
				s := linkPattern.FindStringSubmatch(a)
				// fmt.Println(s)
				if len(s) > 1 {
					rate, _ := strconv.ParseFloat(ratePattern.FindStringSubmatch(a)[1], 32)
					//fmt.Println(rate)
					if strings.Contains(s[1], "douban") {
						movie.Douban = Link{strings.TrimSpace(s[1]), rate}
						if debug {
							log(movie.Douban)
						}
					} else if strings.Contains(s[1], "imdb") {
						movie.Imdb = Link{strings.TrimSpace(s[1]), rate}
						if debug {
							log(movie.Imdb)
						}
					}
				}
			}
		} else {
			value = cleanCommentTag(value)
			switch key {
			case "导演":
				for _, v := range strings.Split(value, "/") {
					movie.Director = append(movie.Director, strings.TrimSpace(v))
				}
				if debug {
					log(movie.Director)
				}
			case "主演":
				for _, v := range strings.Split(value, "/") {
					movie.Charactor = append(movie.Charactor, strings.TrimSpace(v))
				}
				if debug {
					log(movie.Charactor)
				}
			case "类型":
				for _, v := range strings.Split(value, "/") {
					movie.Category = append(movie.Category, strings.TrimSpace(v))
				}
				if debug {
					log(movie.Category)
				}
			case "地区":
				for _, v := range strings.Split(value, "/") {
					movie.District = append(movie.District, strings.TrimSpace(v))
				}
				if debug {
					log(movie.District)
				}
			case "别名":
				for _, v := range strings.Split(value, "/") {
					movie.Alias = append(movie.Alias, strings.TrimSpace(v))
				}
				if debug {
					log(movie.Alias)
				}
			case "片长":
				movie.Length = strings.TrimSpace(value)
				if debug {
					log(movie.Length)
				}
			case "上映时间":
				movie.ReleaseDate = strings.TrimSpace(value)
				if debug {
					log(movie.ReleaseDate)
				}
			}
		}
		table = table[end+5:]
	}
	return movie
}

func save(record_chan chan Movie) {
	record_chan_closed := false
	for {
		if record_chan_closed {
			fmt.Println("save returned")
			finish = true
			return
		}
		// fmt.Println("Waiting for a new movie ...")
		select {
		case record, record_chan_ok := <-record_chan:
			if !record_chan_ok {
				record_chan_closed = true
				fmt.Println(" Record channel closed!")
			} else {
				data, err := json.Marshal(record)
				if err == nil {
					fmt.Println("New movie received", record.Name[0])
					err := ioutil.WriteFile(movie_folder+strings.Replace(record.Name[0], ":", " ", -1), data, os.ModeAppend)
					if err == nil {
						fmt.Println("New movie saved", record.Name[0])
					}
				}
			}
		default:
			fmt.Println("save have a rest")
			time.Sleep(300 * time.Millisecond)
		}
	}
}

func control(fetched_link_chan chan string, todo_link_chan chan string) {
	fmt.Println("start control")
	fetched_link_chan_closed := false

	for {
		if fetched_link_chan_closed || todolinksCnt >= max_count {
			fmt.Println("before close")
			close(todo_link_chan)
			fmt.Println("control returned")
			return
		}
		//fmt.Println("Waiting for a new fetched link ...")
		fmt.Println(" fetched links: ", fetchedlinksCnt, " todo links: ", todolinksCnt)
		//fmt.Println("linkmap size: ", len(linkmap))
		select {
		case fetched_link, fetched_link_chan_ok := <-fetched_link_chan:
			if !fetched_link_chan_ok {
				fetched_link_chan_closed = true
				fmt.Println(" fetched link channel is closed!")
			} else {
				hash_str := gethash(fetched_link)
				hasdone, ok := linkmap[hash_str]
				if !ok || !hasdone {
					fmt.Println("find fetched link: ", fetched_link)
					linkmap[hash_str] = true
					todo_link_chan <- fetched_link
					fmt.Println("send todo link: ", fetched_link)
					todolinksCnt++
				}
			}
		default:
			fmt.Println("control have a rest")
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func crawl(todo_link_chan chan string, page_chan chan Page) {
	todo_link_chan_closed := false
	for {
		if todo_link_chan_closed {
			close(page_chan)
			fmt.Println("crawl returned")
			return
		}
		//fmt.Println("Waiting for a new todo link ...")
		select {
		case todo_link, todo_link_chan_ok := <-todo_link_chan:
			if !todo_link_chan_ok {
				todo_link_chan_closed = true
				fmt.Println(" Todo link channel is closed!")
			} else {
				todo_link = site + todo_link
				fmt.Println(" crawling ", todo_link)
				html := file_get_contents(todo_link)
				fmt.Println(" finish crawling ", todo_link)
				if html != "" {
					page_chan <- Page{todo_link, html}
					fmt.Println(" send page  ", todo_link)
				}
				//time.Sleep(10 * time.Millisecond) //sleep 500ms
			}
		default:
			fmt.Println("crawl have a rest")
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func gethash(str string) string {
	h := sha256.New()
	io.WriteString(h, str)
	return string(h.Sum(nil))
}

func log(a ...interface{}) {
	fmt.Println(a)
}

var finish bool = false

var max_count int64 = 20 * 1000 * 1000

var linkmap map[string]bool = make(map[string]bool)

var fetchedlinksCnt int64 = 0
var todolinksCnt int64 = 0

var debug = false

// var debug = true

func main() {

	fetched_link_chan := make(chan string, max_count)

	todo_link_chan := make(chan string)

	page_chan := make(chan Page)

	record_chan := make(chan Movie)

	go analyze(page_chan, fetched_link_chan, record_chan)

	go save(record_chan)

	go control(fetched_link_chan, todo_link_chan)

	go crawl(todo_link_chan, page_chan)

	url := "/movie/django-unchained/"
	//url := "/"
	//url := "/movie/soom/"
	fetched_link_chan <- url
	fmt.Println("test")
	for {
		if finish {
			return
		}
		time.Sleep(5000 * time.Millisecond)
	}

	//single
	// todo_link := "http://dianying.fm/movie/django-unchained/"
	// todo_link := "http://dianying.fm/movie/ice-age-the-meltdown/"
	// html := file_get_contents(todo_link)
	// page := Page{todo_link, html}
	// fetched_links := fetchLinks(page)
	// fmt.Println(fetched_links)
	// movie := fetchRecord(page)
	// linkMap := make(map[string]Movie)
	// if (len(movie.Name)) > 0 {
	// 	data, err := json.Marshal(movie)
	// 	fmt.Println(data)
	// 	if err == nil {

	// 		err := ioutil.WriteFile(movie_folder+strings.Replace(movie.Name[0], ":", " ", -1), data, os.ModeAppend)
	// 		fmt.Println("New movie saved", movie.Name[0], "|")
	// 		fmt.Println("err", err)
	// 	}
	// 	linkMap[movie.Url] = movie
	// 	data, err = json.Marshal(linkMap)
	// 	fmt.Println(data)
	// 	if err == nil {
	// 		ioutil.WriteFile("movie_single.dat", data, os.ModeAppend)
	// 	}

	// }

}
