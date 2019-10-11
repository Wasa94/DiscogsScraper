import scrapy, pyodbc, logging, re, html2text
from Discogs.items import AlbumData, ArtistData

logging.getLogger().setLevel(logging.INFO) 

class DiscogsDbPipeline(object):
    def get_artist_id(self, name):
        try:
            self._cursor.execute("SELECT Id FROM Artists WHERE Name = ?", name)
            artist_id = self._cursor.fetchone()
            return None if artist_id is None else artist_id[0]
        except Exception:
            self._db.rollback()
            raise Exception("Error getting Artist ID!")

    def store_artist_name(self, name):
        try:
            self._cursor.execute("INSERT INTO Artists(Name) VALUES(?)", name)
            self._db.commit()

            artist_id = self._cursor.execute("SELECT @@IDENTITY AS Id").fetchone()[0]
            return artist_id
        except Exception:
            self._db.rollback()
            raise Exception("Error saving Artist: " + name)

    def store_general_album_info(self, album_name, versions, released, country, artist_id):
        try:
            if released:
                query_data = (artist_id, album_name, versions, released, country)
                self._cursor.execute("INSERT INTO Albums(ArtistId, Name, Versions, Year, Country) VALUES (?,?,?,?,?)", query_data)
            else:
                query_data = (artist_id, album_name, versions, country)
                self._cursor.execute("INSERT INTO Albums(ArtistId, Name, Versions, Country) VALUES (?,?,?,?)", query_data)

            self._db.commit()

            album_id = self._cursor.execute("SELECT @@IDENTITY AS Id").fetchone()[0]
            return album_id
        except Exception:
            self._db.rollback()
            raise Exception("Error saving Album: " + album_name)

    def format_profile(self, strings):
        result = []
        regex = re.compile(r'[\n\r\t]')
        for s in strings:
            tmp = regex.sub(" ", s).strip()
            result.append(tmp)

        return result

    def store_profile(self, profile, album_id):
        for trait in profile:
            try:
                if trait == 'Style':
                    styles = [x.strip() for x in profile[trait].split(',')]
                    styles = self.format_profile(styles)
                    for style in styles:
                        if style == '':
                            continue
                        self._cursor.execute("INSERT INTO AlbumStyles(AlbumId, Name) VALUES(?, ?)", (album_id, style))
                        self._db.commit()
                elif trait == 'Genre':
                    genres = [x.strip() for x in profile[trait].split(',')]
                    genres = self.format_profile(genres)
                    tmp = ["Folk", "World", "& Country"]
                    if all(item in genres for item in tmp):
                        genres.remove("Folk")
                        genres.remove("World")
                        genres.remove("& Country")
                        genres.append("Folk, World, & Country")
                    for genre in genres:
                        if genre == '':
                            continue
                        self._cursor.execute("INSERT INTO AlbumGenres(AlbumId, Name) VALUES(?, ?)", (album_id, genre))
                        self._db.commit()
                elif trait == 'Format':
                    album_format = profile[trait].strip()
                    self._cursor.execute("UPDATE Albums SET Format = ? WHERE Id = ?", (album_format, album_id))
                    self._db.commit()

            except Exception:
                logging.error("Error saving profile for album id: " + str(album_id))
                self._db.rollback()

    def get_sec(self, time_str):
        lst = time_str.split(':')
        if len(lst) == 2:
            return int(lst[0]) * 60 + int(lst[1])
        elif len(lst) == 3:
            return int(lst[0]) * 3600 + int(lst[1]) * 60 + int(lst[0])
        else:
            return None

    def store_tracks(self, tracks, album_id):
        for track in tracks:
            try:
                track_name = track[0]

                if track[1] is not None:
                    duration = self.get_sec(track[1])
                else:
                    duration = None

                authors = track[2]
                if 'Arranged By' in authors:
                    arranger = self._artist_id_for_name(authors['Arranged By'])
                else:
                    arranger = None
                if 'Music By' in authors:
                    music = self._artist_id_for_name(authors['Music By'])
                else:
                    music = None
                if 'Lyrics By' in authors:
                    lyrics = self._artist_id_for_name(authors['Lyrics By'])
                else:
                    lyrics = None
                
                self._cursor.execute("INSERT INTO Tracks(AlbumId, Name, Duration, ArrangedBy, MusicBy, LyricsBy) VALUES(?, ?, ?, ?, ?, ?)", (album_id, track_name, duration, arranger, music, lyrics))
                self._db.commit()
            except Exception:
                logging.error("Error storing track for album id: " + str(album_id))
                self._db.rollback()

    def _artist_id_for_name(self, artist_name):
        artist_id = self.get_artist_id(artist_name)

        if artist_id is None:
            artist_id = self.store_artist_name(artist_name)
        return artist_id

    def open_spider(self, spider):
        self._db = pyodbc.connect("Driver={SQL Server};Server=localhost\\SQLEXPRESS;Database=Discogs;Trusted_Connection=yes;")
        self._cursor = self._db.cursor()


    def close_spider(self, spider):
        self._db.close()

    def store_artist(self, item):   
        try:
            artist_id = self._artist_id_for_name(item['artist_name'])
            self._cursor.execute("UPDATE Artists SET Credits = ?, Vocals = ?, Sites = ? WHERE Id = ?",
                (item['artist_credits'], item['artist_vocals'], item['sites'], artist_id))
            self._db.commit()
        except Exception as ex:
            logging.error(ex)
            self._db.rollback()

    def process_item(self, item, spider):
        try:
            if type(item) is ArtistData:
                self.store_artist(item)
            else:
                artist_id = self._artist_id_for_name(item['artist_name'])
                country = item['profile'].get('Country', '')

                if 'Released' in item['profile']:
                    released = item['profile']['Released'].strip()
                else:
                    released = item['profile']['Year'].strip()

                released = [int(s) for s in re.findall(r'\d+', released.strip())]
                if released != []:
                    released = released[len(released) - 1]
                else:
                    released = -1
                
                album_id = self.store_general_album_info(item['album_name'], item['album_version'], released,  country, artist_id)

                self.store_profile(item['profile'], album_id)
                self.store_tracks(item['track_list'], album_id)
        except Exception as error:
            logging.error("Error: " + error + " - " + item['album_name'])

        return item


class DiscogsPipeline(object):
    def parse_artist_name(self, response):
        res = None
        try:
            res = response.xpath('//div[@class="profile"]/h1/span[1]/span/a/text()').extract()[0].strip()
        except:
            logging.error("Error: Parsing artist name")

        return res

    def parse_album_name(self, response):
        res = None
        try:
            res = response.xpath('//div[@class="profile"]/h1/span[2]/text()').extract()[0].strip()
        except:
            logging.error("Error: Parsing album name")

        return res

    def parse_profile(self, response):
        header_list = response.selector.xpath("//div[@class='profile']/div[@class='head']/text()").extract()
        content_selectors = response.selector.xpath("//div[@class='profile']/div[@class='content']")

        if len(header_list) != len(content_selectors):
            return {}

        converter = html2text.HTML2Text()
        converter.ignore_links = True

        data = {}
        for i in range (0, len(content_selectors)):
            contend = converter.handle(content_selectors[i].extract())
            data[header_list[i].replace(':', '')] = str.strip(contend)

        return data

    def parse_track_list(self, response):
        tracklist_selectors = response.selector.xpath("//tr[@class=' tracklist_track track']")
        tracklist_selectors.append(response.selector.xpath("//tr[@class='first tracklist_track track']"))

        data = []
        for selector in  tracklist_selectors:
            title = selector.xpath("./td[@class='track tracklist_track_title '] |  ./td[@class='track tracklist_track_title mini_playlist_track_has_artist']").xpath("./a/span/text() | ./span/text()").extract()
            
            writtings = selector.xpath("./td[@class='track tracklist_track_title '] |  ./td[@class='track tracklist_track_title mini_playlist_track_has_artist']").xpath("./blockquote/span/text()").extract()
            authors = selector.xpath("./td[@class='track tracklist_track_title '] |  ./td[@class='track tracklist_track_title mini_playlist_track_has_artist']").xpath("./blockquote/span/a/text()").extract()
            writtings = [s for s in writtings if s.find('*') == -1]
            i = 0
            d = {}
            for s in writtings:
                if s.find('Arranged By') != -1:
                    d['Arranged By'] = authors[i]
                if s.find('Lyrics By') != -1:
                    d['Lyrics By'] = authors[i]
                if s.find('Music By') != -1:
                    d['Music By'] = authors[i]
                i += 1

            if title == []:
                continue
            title = title[0].strip()

            duration = selector.xpath("./td[@class='tracklist_track_duration']/span/text()").extract()
            if duration != []:
                duration = duration[0].strip()
            else:
                duration = None

            data.append([title, duration, d])

        return data


    def parse_album_versions(self, response):
        versions = response.selector.xpath('//div[@id="m_versions"]/h3/text()').extract()
        if versions != []:
            versions = [int(s) for s in re.findall(r'\d+', versions[0].strip())]
            if len(versions) == 1:
                versions = versions[0]
            elif len(versions) == 2:
                versions = versions[1] + 1
            else:
                versions = 1
        else:
            versions = 1
        return versions

    def parse_artist(self, response):
        return response.xpath('//div[@class="profile"]/h1/text()').extract()[0]
    
    def parse_sites(self, response):
        sites = None
        try:
            header_list = response.selector.xpath("//div[@class='profile']/div[@class='head']/text()").extract()
            content_selectors = response.selector.xpath("//div[@class='profile']/div[@class='content']")

            if len(header_list) != len(content_selectors):
                return {}

            converter = html2text.HTML2Text()
            converter.ignore_links = True

            data = {}
            for i in range (0, len(content_selectors)):
                contend = converter.handle(content_selectors[i].extract())
                data[header_list[i].replace(':', '')] = str.strip(contend)

            sites = data.get('Sites', None)
        except:
            pass

        return sites

    def parse_credits(self, response):
        album_credits = response.xpath('//a[@data-credit-type="Credits" and @data-credit-subtype="All"]/span/text()').extract()
        if album_credits != []:
            album_credits = int(album_credits[0])
        else:
            album_credits = 0
        return album_credits
    
    def parse_vocals(self, response):
        vocals = response.xpath('//a[@data-credit-type="Credits" and @data-credit-subtype="Vocals"]/span/text()').extract()
        if vocals != []:
            vocals = int(vocals[0])
        else:
            vocals = 0
        return vocals

    def process_item(self, item, spider):
        if item.get('artist', '') != '':
            artist = ArtistData()

            artist['artist_name'] = self.parse_artist(item['artist'])
            artist['sites'] = self.parse_sites(item['artist'])
            artist['artist_credits'] = self.parse_credits(item['artist'])
            artist['artist_vocals'] = self.parse_vocals(item['artist'])
            
            return artist
        else:
            album = AlbumData()

            album['artist_name'] = self.parse_artist_name(item['album'])
            album['album_name'] = self.parse_album_name(item['album'])
            album['profile'] = self.parse_profile(item['album'])
            album['album_version'] = self.parse_album_versions(item['album'])
            album['track_list'] = self.parse_track_list(item['album'])

            return album